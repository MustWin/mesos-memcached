

= Memcache framework for Mesos

This framework is an early implementation of an autoscaling memcache cluster.

The executor starts memcache containers using docker and monitors their CPU and memory usage.
When either metric passes a given threshold, a new container is allocated by the scheduler.

For now, the list of allocated memcached services is exposed on scheduler_box:10001

To prevent totally destroying your cache, the rate that new memcached servers are allocated is throttled.

= Build it
go get
./build.sh

= Run it locally - these work on OS X. Linux should be easier.
1) Install docker
2_) Make sure your docker client on the slave can connect properly. You may have to export some env vars. E.g.
```
export DOCKER_HOST=tcp://192.168.99.100:2376
export DOCKER_CERT_PATH=/Users/Mike/.docker/machine/machines/default
export DOCKER_TLS_VERIFY=1
```

3) run mesos-master
4) run mesos-slave connected to the master
5) run the scheduler
```
./pkg/scheduler -master ip.addr.here:port
./bin/test.rb
```

Watch your memcache containers allocate themselves automagically as the memory gets used.


= Future work

* Add more parameters to the scheduler so you can fiddle with the knobs without editing source code.
* Integrate with something like https://github.com/facebook/mcrouter to improve cache transitions
* Production readiness: 
** A memcached container that doesn't cap out at 64mb
** Better service discovery
** Monitor the state of the running docker containers, sample code:

cmd := exec.Command("sleep", "2")
        err := make(chan error)

        // run the command and wait for it in a seperate goroutine.
    go func() {
                 err <- cmd.Run()
        }()

        // sleep for a little bit.
        time.Sleep(nwait)

        // check the status of the process.
    select {
        default:
                log.Print("process still running")
        case e := <-err:
                if e != nil {
                         log.Print("process exited: %s", e)
                
                } else {
                        log.Print("process exited without error")
                
                }
        }>
    }>
    }
