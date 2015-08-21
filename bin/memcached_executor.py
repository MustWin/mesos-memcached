#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import threading
from threading import Thread
import time

import urlparse, urllib, sys
from bs4 import BeautifulSoup

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2

class Result(object):
    def __repr__(self):
        return json.dumps(self.__dict__, sort_keys = True)

class MemcacheMessage(Result):
    """A memcache status update
    must serialize to JSON as its default representation:
    >>> res = MemcacheMessage(
    ...     "1234",
    ...     "45"
    ...     "50"
    ... )
    >>> repr(res)
    '{"cpu": 50, "taskId": "1234", "memory": "45"}'
    """
    def __init__(self, taskId, memory, cpu):
        self.taskId = taskId
        self.memory = memory
        self.cpu = cpu

class MemcacheExecutor(Executor):
    def __init__(self):
        self.threads = {}

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
      print "MemcacheExecutor registered"

    def reregistered(self, driver, slaveInfo):
      print "MemcacheExecutor reregistered"

    def disconnected(self, driver):
      print "MemcacheExecutor disconnected"

    def launchTask(self, driver, task):
        def run_task():
            print "running memcache"
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            try:
                # Start memcache
            except:
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_FAILED
                update.message = error_msg

                driver.sendStatusUpdate(update)
                print error_msg
                return


            # AND THE MEAT
            while(true)
                  # Send memcache status

                res = results.MemcacheMessage(
                  task.task_id.value,
                  url,
                  links
                )
                message = repr(res)
                driver.sendFrameworkMessage(message)
                time.sleep(5000)
            #print "Sending status update..."
            #update = mesos_pb2.TaskStatus()
            #update.task_id.value = task.task_id.value
            #update.state = mesos_pb2.TASK_FINISHED
            #driver.sendStatusUpdate(update)
            #print "Sent status update"
            return

        thread = threading.Thread(target=run_task, args=(i,))
        self.threads[task.task_id] = t
        thread.start()

    def killTask(self, driver, taskId):
      shutdown(self, driver)

    def frameworkMessage(self, driver, message):
      print "Ignoring framework message: %s" % message

    def shutdown(self, driver):
      print "Shutting down"
      sys.exit(0)

    def error(self, error, message):
      pass


if __name__ == "__main__":
    print "Starting Launching Executor (LE)"
    driver = MesosExecutorDriver(CrawlExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
