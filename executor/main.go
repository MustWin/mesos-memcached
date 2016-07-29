package main

// EXECUTOR

import (
	//"flag"
	"encoding/json"
	"fmt"
	"github.com/Mustwin/mesos-memcached/util"
	"github.com/mesos/mesos-go/executor"
	"github.com/mesos/mesos-go/mesosproto"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	MEMCACHE_CONTAINER_PREFIX = "memcached-"
)

type Task struct {
	TaskID       *mesosproto.TaskID
	MemcachedCmd *exec.Cmd
	ContainerID  string
	Stopped      bool
}

type MemcacheExecutor struct {
	Tasks    map[string]*Task
	Hostname *string
}

func NewMemcacheExecutor() *MemcacheExecutor {
	return &MemcacheExecutor{Tasks: make(map[string]*Task)}
}

/**
 * Invoked once the executor driver has been able to successfully
 * connect with Mesos. In particular, a scheduler can pass some
 * data to its executors through the FrameworkInfo.ExecutorInfo's
 * data field.
 */
func (ref *MemcacheExecutor) Registered(_ executor.ExecutorDriver, _ *mesosproto.ExecutorInfo, _ *mesosproto.FrameworkInfo, slaveInfo *mesosproto.SlaveInfo) {
	ref.Hostname = slaveInfo.Hostname
	fmt.Println("Memcached Framework registered")
}

/**
 * Invoked when the executor re-registers with a restarted slave.
 */
func (ref *MemcacheExecutor) Reregistered(_ executor.ExecutorDriver, _ *mesosproto.SlaveInfo) {
	fmt.Println("Memcached Framework registered")
}

/**
 * Invoked when the executor becomes "disconnected" from the slave
 * (e.g., the slave is being restarted due to an upgrade).
 */
func (ref *MemcacheExecutor) Disconnected(_ executor.ExecutorDriver) {
	fmt.Println("Memcached Framework unregistered")
}

/* Private helper, parses output like this:
CONTAINER           CPU %               MEM USAGE/LIMIT     MEM %               NET I/O
memcache-2          0.01%               790.5 kB/2.1 GB     0.04%               648 B/738 B
*/
func parseContainerStatus(taskId string, output []byte) util.MemcacheStatus {
	regex := regexp.MustCompile("\\d+\\.\\d+%")
	line2 := strings.Split(string(output), "\n")[1]
	results := regex.FindAll([]byte(line2), 2)
	cpu, _ := strconv.ParseFloat(strings.Replace(string(results[0]), "%", "", 1), 64)
	memory, _ := strconv.ParseFloat(strings.Replace(string(results[1]), "%", "", 1), 64)
	return util.MemcacheStatus{
		CPU:    cpu,
		Memory: memory,
		TaskID: taskId,
	}
}

/**
 * Invoked when a task has been launched on this executor (initiated
 * via SchedulerDriver.LaunchTasks). Note that this task can be realized
 * with a goroutine, an external process, or some simple computation, however,
 * no other callbacks will be invoked on this executor until this
 * callback has returned.
 */
func (ref *MemcacheExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesosproto.TaskInfo) {
	fmt.Println(taskInfo)
	fmt.Println(taskInfo.Resources)
	var port *uint64
	var mem *float64
	for _, resource := range taskInfo.Resources {
		switch resource.GetName() {
		case "ports":
			port = resource.GetRanges().GetRange()[0].Begin
		case "mem":
			mem = resource.GetScalar().Value
		}
	}
	fmt.Println("Port: ", *port)
	ref.Tasks[taskInfo.TaskId.GetValue()] = &Task{Stopped: false, MemcachedCmd: nil, TaskID: taskInfo.TaskId, ContainerID: ""}

	go func() {
		update := &mesosproto.TaskStatus{
			TaskId: taskInfo.TaskId,
			State:  mesosproto.TaskState_TASK_RUNNING.Enum(),
			Source: mesosproto.TaskStatus_SOURCE_EXECUTOR.Enum(),
			//Message: "Started Memcached",
			SlaveId:    taskInfo.SlaveId,
			ExecutorId: taskInfo.Executor.ExecutorId,
			//Timestamp: &float64(time.Now().Unix()),
			//Reason     *TaskStatus_Reason `protobuf:"varint,10,opt,name=reason,enum=mesosproto.TaskStatus_Reason" json:"reason,omitempty"`
			//  Data       []byte             `protobuf:"bytes,3,opt,name=data" json:"data,omitempty"`
		}
		driver.SendStatusUpdate(update)

		// Start Memcached
		ref.Tasks[taskInfo.TaskId.GetValue()].ContainerID = MEMCACHE_CONTAINER_PREFIX + taskInfo.TaskId.GetValue()
		cmd := exec.Command("docker", "run", "--name", ref.Tasks[taskInfo.TaskId.GetValue()].ContainerID, "-m", strconv.Itoa(int(*mem))+"m", "--net", "bridge", "-p", strconv.Itoa(int(*port))+":11211", util.MEMCACHE_CONTAINER)
		ref.Tasks[taskInfo.TaskId.GetValue()].MemcachedCmd = cmd
		err := cmd.Start()

		if err != nil {
			time := float64(time.Now().Unix())
			errStr := err.Error()
			update.Timestamp = &time
			update.State = mesosproto.TaskState_TASK_FAILED.Enum()
			update.Message = &errStr
			driver.SendStatusUpdate(update)
			return
		}

		go func() {
			for !ref.Tasks[taskInfo.TaskId.GetValue()].Stopped {
				time.Sleep(10 * time.Second)
				output, err := exec.Command("docker", "stats", "--no-stream", MEMCACHE_CONTAINER_PREFIX+taskInfo.TaskId.GetValue()).Output()
				fmt.Println("Output: ", string(output))
				if err != nil {
					fmt.Println("Error fetching stats: ", output, err)
				} else {
					status := parseContainerStatus(taskInfo.TaskId.GetValue(), output)
					status.Hostname = *ref.Hostname
					status.Port = int(*port)
					stringStatus, _ := json.Marshal(status)
					driver.SendFrameworkMessage(string(stringStatus))
				}
			}
		}()
		err = cmd.Wait()

	}()
}

/**
 * Invoked when a task running within this executor has been killed
 * (via SchedulerDriver.KillTask). Note that no status update will
 * be sent on behalf of the executor, the executor is responsible
 * for creating a new TaskStatus (i.e., with TASK_KILLED) and
 * invoking ExecutorDriver.SendStatusUpdate.
 */
func (ref *MemcacheExecutor) KillTask(driver executor.ExecutorDriver, taskID *mesosproto.TaskID) {
	ref.Tasks[taskID.GetValue()].Stopped = true
	ref.Tasks[taskID.GetValue()].MemcachedCmd.Process.Signal(syscall.SIGTERM)
	//go func() {
	// time.Sleep(3 * time.Second)
	// Clean up the container from the worker
	fmt.Println("Removing container: ", ref.Tasks[taskID.GetValue()].ContainerID)
	out, err := exec.Command("docker", "rm", "-f", ref.Tasks[taskID.GetValue()].ContainerID).Output()
	if err != nil {
		fmt.Println(out, err)
	}
	delete(ref.Tasks, taskID.GetValue())
	update := &mesosproto.TaskStatus{
		TaskId: taskID,
		State:  mesosproto.TaskState_TASK_KILLED.Enum(),
		Source: mesosproto.TaskStatus_SOURCE_EXECUTOR.Enum(),
		//Message: "Started Memcached",
		//Timestamp: &float64(time.Now().Unix()),
		//Reason     *TaskStatus_Reason `protobuf:"varint,10,opt,name=reason,enum=mesosproto.TaskStatus_Reason" json:"reason,omitempty"`
		//  Data       []byte             `protobuf:"bytes,3,opt,name=data" json:"data,omitempty"`
	}
	driver.SendStatusUpdate(update)
	//}()
}

/**
 * Invoked when a framework message has arrived for this
 * executor. These messages are best effort; do not expect a
 * framework message to be retransmitted in any reliable fashion.
 */
func (ref *MemcacheExecutor) FrameworkMessage(_ executor.ExecutorDriver, _ string) {
	fmt.Println("Framework message ignored")
}

/**
 * Invoked when the executor should terminate all of its currently
 * running tasks. Note that after Mesos has determined that an
 * executor has terminated, any tasks that the executor did not send
 * terminal status updates for (e.g., TASK_KILLED, TASK_FINISHED,
 * TASK_FAILED, etc) a TASK_LOST status update will be created.
 */
func (ref *MemcacheExecutor) Shutdown(driver executor.ExecutorDriver) {
	fmt.Println("Running Executor Shutdown")
	for _, task := range ref.Tasks {
		ref.KillTask(driver, task.TaskID)
	}
}

/**
 * Invoked when a fatal error has occured with the executor and/or
 * executor driver. The driver will be aborted BEFORE invoking this
 * callback.
 */
func (ref *MemcacheExecutor) Error(driver executor.ExecutorDriver, err string) {
	fmt.Println("FATAL ERROR: ", err)
}

func main() {
	//master := flag.String("master", "10.0.0.26:5050", "Location of leading Mesos master")

	//flag.Parse()

	config := executor.DriverConfig{
		Executor: NewMemcacheExecutor(),
	}

	driver, err := executor.NewMesosExecutorDriver(config)
	if err != nil {
		fmt.Println("Driver initialization failed: ", err)
		os.Exit(1)
	}

	// Catch interrupt
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		s := <-c
		if s != os.Interrupt {
			return
		}

		fmt.Println("Memcache is shutting down")
		config.Executor.Shutdown(driver)
		driver.Stop()
	}()

	fmt.Println("Starting driver")
	if status, err := driver.Run(); err != nil {
		fmt.Printf("Executor stopped with status %s and error: %s\n", status.String(), err.Error())
	}
	config.Executor.Shutdown(driver)
	fmt.Println("Exiting...")
}
