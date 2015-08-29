package util

import (
    mesos "github.com/mesos/mesos-go/mesosproto"
)

var (
    MEMCACHE_CONTAINER = "sameersbn/memcached:latest"
)

type MemcacheStatus struct {
  TaskID string `json:task_id`
  CPU float64 `json:cpu`
  Memory float64 `json:memory`
  Hostname string `json:hostname`
  Port int `json:port`
}

// NameFor returns the string name for a TaskState.
func NameFor(state *mesos.TaskState) string {
    switch *state {
    case mesos.TaskState_TASK_STAGING:
        return "TASK_STAGING"
    case mesos.TaskState_TASK_STARTING:
        return "TASK_STARTING"
    case mesos.TaskState_TASK_RUNNING:
        return "TASK_RUNNING"
    case mesos.TaskState_TASK_FINISHED:
        return "TASK_FINISHED" // TERMINAL
    case mesos.TaskState_TASK_FAILED:
        return "TASK_FAILED" // TERMINAL
    case mesos.TaskState_TASK_KILLED:
        return "TASK_KILLED" // TERMINAL
    case mesos.TaskState_TASK_LOST:
        return "TASK_LOST" // TERMINAL
    default:
        return "UNKNOWN"
    }
}

// IsTerminal determines if a TaskState is a terminal state, i.e. if it singals
// that the task has stopped running.
func IsTerminal(state *mesos.TaskState) bool {
    switch *state {
    case mesos.TaskState_TASK_FINISHED,
        mesos.TaskState_TASK_FAILED,
        mesos.TaskState_TASK_KILLED,
        mesos.TaskState_TASK_LOST:
        return true
    default:
        return false
    }
}
