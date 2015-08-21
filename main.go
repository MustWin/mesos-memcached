package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "github.com/golang/protobuf/proto"
    mesos "github.com/mesos/mesos-go/mesosproto"
    "github.com/mesos/mesos-go/mesosutil"
    "github.com/Mustwin/mesos-memcached/rendler"
    sched "github.com/mesos/mesos-go/scheduler"
    "log"
    "os"
    "os/signal"
    "path/filepath"
    "time"
)

const (
    TASK_CPUS = 0.3
    TASK_MEM = 100.0
    SHUTDOWN_TIMEOUT = time.Duration(15) * time.Second
    TASK_AVG_MEM_MAX = 0.80 * TASK_MEM
    TASK_AVG_CPU_MAX = 0.80 * TASK_CPUS
)

const (
    MEMCACHE_COMMAND = "python memcached_executor.py"
)

var (
    defaultFilter = &mesos.Filters{RefuseSeconds: proto.Float64(1)}
)

// maxTasksForOffer computes how many tasks can be launched using a given offer
func canLaunchNewTask(offer *mesos.Offer) bool {
    var cpus, mem float64

    for _, resource := range offer.Resources {
        switch resource.GetName() {
        case "cpus":
            cpus += *resource.GetScalar().Value
        case "mem":
            mem += *resource.GetScalar().Value
        }
    }

    if cpus >= TASK_CPUS && mem >= TASK_MEM {
        return true
    }
    return false
}

type MemcacheStatus struct {
  TaskID string `json:task_id`
  CPU int `json:cpu`
  Memory int `json:memory`
}

// MemcacheScheduler implements the Scheduler interface and stores
// the state needed for Memcache to function.
type MemcacheScheduler struct {
    tasksCreated int
    tasksRunning int

    minTasks int
    taskStatuses map[string]*MemcacheStatus

    memcacheExecutor  *mesos.ExecutorInfo

    // This channel is closed when the program receives an interrupt,
    // signalling that the program should shut down.
    shutdown chan struct{}
    // This channel is closed after shutdown is closed, and only when all
    // outstanding tasks have been cleaned up
    done chan struct{}
}

// newMemcacheScheduler creates a new scheduler for Memcache
func newMemcacheScheduler(minTasks int) *MemcacheScheduler {
    memcacheArtifacts := executorURIs()

    actualCommand := MEMCACHE_COMMAND
    var port = uint32(11211)
    s := &MemcacheScheduler{
        taskStatuses: make(map[string]*MemcacheStatus),
        minTasks: minTasks,
        memcacheExecutor: &mesos.ExecutorInfo{
            ExecutorId: &mesos.ExecutorID{Value: proto.String("memcache-executor")},
            Command: &mesos.CommandInfo{
                Value: proto.String(actualCommand),
                Uris:  memcacheArtifacts,
            },
            Discovery: &mesos.DiscoveryInfo{
                Visibility: mesos.DiscoveryInfo_EXTERNAL.Enum(),
                Ports: &mesos.Ports{Ports: []*mesos.Port{&mesos.Port{Number: &port}}},
            },
            Name: proto.String("Memcache"),
        },

        shutdown: make(chan struct{}),
        done:     make(chan struct{}),
    }
    return s
}

func memcacheAvgCPU(statuses map[string]*MemcacheStatus) float64 {
  var total = 0;
  for _, status := range(statuses) {
    total += status.CPU
  }
  return float64(total) / float64(len(statuses))
}
func memcacheAvgMemory(statuses map[string]*MemcacheStatus) float64 {
  var total = 0;
  for _, status := range(statuses) {
    total += status.Memory
  }
  return float64(total) / float64(len(statuses))
}

func (s *MemcacheScheduler) shouldLaunchNewTask() bool {
    if s.tasksRunning == 0 {
        return true
    } else if s.tasksRunning < s.minTasks {
        return true
    } else if memcacheAvgMemory(s.taskStatuses) > TASK_AVG_CPU_MAX || memcacheAvgCPU(s.taskStatuses) > TASK_AVG_CPU_MAX {
        return true
    }
    return false
}

func (s *MemcacheScheduler) newTaskPrototype(offer *mesos.Offer) *mesos.TaskInfo {
    taskID := s.tasksCreated
    s.tasksCreated++
    return &mesos.TaskInfo{
        TaskId: &mesos.TaskID{
            Value: proto.String(fmt.Sprintf("Memcache-%d", taskID)),
        },
        SlaveId: offer.SlaveId,
        Resources: []*mesos.Resource{
            mesosutil.NewScalarResource("cpus", TASK_CPUS),
            mesosutil.NewScalarResource("mem", TASK_MEM),
        },
    }
}

func (s *MemcacheScheduler) newMemcacheTask(offer *mesos.Offer) *mesos.TaskInfo {
    task := s.newTaskPrototype(offer)
    task.Name = proto.String("MEMCACHE_" + *task.TaskId.Value)
    task.Executor = s.memcacheExecutor
    task.Data = nil
    return task
}

func (s *MemcacheScheduler) Registered(
    _ sched.SchedulerDriver,
    frameworkID *mesos.FrameworkID,
    masterInfo *mesos.MasterInfo) {
    log.Printf("Framework %s registered with master %s", frameworkID, masterInfo)
}

func (s *MemcacheScheduler) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
    log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *MemcacheScheduler) Disconnected(sched.SchedulerDriver) {
    log.Println("Framework disconnected with master")
}

func (s *MemcacheScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
    log.Printf("Received %d resource offers", len(offers))
    for _, offer := range offers {
        select {
        case <-s.shutdown:
            log.Println("Shutting down: declining offer on [", offer.Hostname, "]")
            driver.DeclineOffer(offer.Id, defaultFilter)
            if s.tasksRunning == 0 {
                close(s.done)
            }
            continue
        default:
        }

        tasks := []*mesos.TaskInfo{}
        if canLaunchNewTask(offer) && s.shouldLaunchNewTask() {
            task := s.newMemcacheTask(offer)
            tasks = append(tasks, task)
        }

        if len(tasks) == 0 {
            driver.DeclineOffer(offer.Id, defaultFilter)
        } else {
            driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, defaultFilter)
        }
    }
}

func (s *MemcacheScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
    log.Printf("Received task status [%s] for task [%s]", rendler.NameFor(status.State), *status.TaskId.Value)

    if *status.State == mesos.TaskState_TASK_RUNNING {
        s.tasksRunning++
    } else if rendler.IsTerminal(status.State) {
        s.tasksRunning--
        if s.tasksRunning == 0 {
            select {
            case <-s.shutdown:
                close(s.done)
            default:
            }
        }
    }
}

func (s *MemcacheScheduler) FrameworkMessage(
    driver sched.SchedulerDriver,
    executorID *mesos.ExecutorID,
    slaveID *mesos.SlaveID,
    message string) {

    log.Println("Getting a framework message")
    switch *executorID.Value {
    case *s.memcacheExecutor.ExecutorId.Value:
        log.Print("Received framework message from memcache")
        var result MemcacheStatus
        err := json.Unmarshal([]byte(message), &result)
        if err != nil {
            log.Printf("Error deserializing MemcacheResult: [%s]", err)
            return
        }
        s.taskStatuses[result.TaskID] = &result

    default:
        log.Printf("Received a framework message from some unknown source: %s", *executorID.Value)
    }
}

func (s *MemcacheScheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
    log.Printf("Offer %s rescinded", offerID)
}
func (s *MemcacheScheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
    log.Printf("Slave %s lost", slaveID)
}
func (s *MemcacheScheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
    log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (s *MemcacheScheduler) Error(_ sched.SchedulerDriver, err string) {
    log.Printf("Receiving an error: %s", err)
}

func executorURIs() []*mesos.CommandInfo_URI {
    basePath, err := filepath.Abs(filepath.Dir(os.Args[0]) + "/../..")
    if err != nil {
        log.Fatal("Failed to find the path to MemcacheExecutor")
    }
    baseURI := fmt.Sprintf("%s/", basePath)

    pathToURI := func(path string, extract bool) *mesos.CommandInfo_URI {
        return &mesos.CommandInfo_URI{
            Value:   &path,
            Extract: &extract,
        }
    }

    return []*mesos.CommandInfo_URI{
        pathToURI(baseURI+"bin/memcached_executor.py", false),
    }
}

func main() {
    minTasks := flag.Int("min", 1, "The start size of the cluster")
    master := flag.String("master", "10.141.141.10:5050", "Location of leading Mesos master")

    flag.Parse()

    scheduler := newMemcacheScheduler(*minTasks)
    driver, err := sched.NewMesosSchedulerDriver(sched.DriverConfig{
        Master: *master,
        Framework: &mesos.FrameworkInfo{
            Name: proto.String("MEMCACHE"),
            User: proto.String(""),
        },
        Scheduler: scheduler,
    })
    if err != nil {
        log.Printf("Unable to create scheduler driver: %s", err)
        return
    }

    // Catch interrupt
    go func() {
        c := make(chan os.Signal, 1)
        signal.Notify(c, os.Interrupt, os.Kill)
        s := <-c
        if s != os.Interrupt {
            return
        }

        log.Println("Memcache is shutting down")
        close(scheduler.shutdown)

        select {
        case <-scheduler.done:
        case <-time.After(SHUTDOWN_TIMEOUT):
        }

        // we have shut down
        driver.Stop(false)
    }()

    log.Printf("Starting driver")
    if status, err := driver.Run(); err != nil {
        log.Printf("Framework stopped with status %s and error: %s\n", status.String(), err.Error())
    }
    log.Println("Exiting...")
}
