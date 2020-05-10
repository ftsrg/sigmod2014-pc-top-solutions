/*
Copyright 2014 Moritz Kaufmann, Manuel Then, Tobias Muehlbauer, Andrey Gubichev

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "include/concurrent/scheduler.hpp"

#include <iostream>
#include "include/concurrent/atomic.hpp"
#include <chrono>
#include <pthread.h>
#include <sched.h>
#include "include/util/log.hpp"

///--- Scheduler related methods
Scheduler::Scheduler(awfy::counters::ProgramCounters& counters) 
   : ioTasks(), workTasks(), closeOnEmpty(false), currentlyEmpty(false), nextTaskId(0), counters(counters) {
}

Scheduler::~Scheduler() {
   if(currentlyEmpty) {
      counters.endStalledScheduler();
   }
   assert(ioTasks.size()==0);
   assert(workTasks.size()==0);
}

void Scheduler::schedule(const std::vector<Task>& funcs, Priorities::Priority priority, bool isIO) {
   awfy::lock_guard<awfy::Mutex> lock(taskMutex);
   for (unsigned i = 0; i < funcs.size(); ++i) {
      Task* task = new Task(funcs[i]);
      #ifdef DEBUG
      counters.scheduledTasks++;
      counters.countScheduledTask();
      #endif
      if(isIO) {
         ioTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
      } else {
         workTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
      }
   }
   if(currentlyEmpty) {
      currentlyEmpty=false;
      counters.endStalledScheduler();
   }
   taskCondition.signal();
}

void Scheduler::schedule(const Task& scheduleTask, Priorities::Priority priority, bool isIO) {
   awfy::lock_guard<awfy::Mutex> lock(taskMutex);
   Task* task = new Task(scheduleTask);
   #ifdef DEBUG
   counters.scheduledTasks++;
   counters.countScheduledTask();
   #endif
   if(isIO) {
      ioTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
   } else {
      workTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
   }
   if(currentlyEmpty) {
      currentlyEmpty=false;
      counters.endStalledScheduler();
   }
   taskCondition.signal();
}

Task* Scheduler::getTask(bool preferIO) {
   taskMutex.lock();
   while(true) {
      // Try to acquire task
      if(!ioTasks.empty()||!workTasks.empty()) {
         Task* task;
         if((preferIO && !ioTasks.empty()) || workTasks.empty()) {
            task = ioTasks.top().second;
            ioTasks.pop();
         } else {
            task = workTasks.top().second;
            workTasks.pop();
         }
         assert(task!=nullptr);
         auto numTasks=ioTasks.size()+workTasks.size();
         if(numTasks==0&&!closeOnEmpty) {
            currentlyEmpty=true;
            counters.startStalledScheduler();
         }
         taskMutex.unlock();
         if(numTasks>0) { taskCondition.signal(); }
         return task;
      } else {
         // Wait if no task is available
         if(closeOnEmpty) {
            taskMutex.unlock();
            taskCondition.signal();
            break;
         } else {
            taskCondition.wait(taskMutex.get());
         }
      }
   }

   return nullptr;
}

void Scheduler::setCloseOnEmpty() {
   closeOnEmpty=true;
   taskCondition.broadcast();
}

size_t Scheduler::size() {
   return ioTasks.size()+workTasks.size();
}

// Executor related implementation
void Executor::run() {
   // set thread affinity to specific core (core pinning)
   counters.initThread();
   while(true) {
      counters.startStalled();
      auto task = scheduler.getTask(preferIO);
      counters.endStalled();
      if(task==nullptr) { break; }

      counters.startTask(task->groupId);
      task->execute();
      counters.endTask();
      delete task;
   }
}

void* Executor::start(void* argument) {
   Executor* executor = static_cast<Executor*>(argument);
   executor->run();
   return nullptr;
}

// Taskgroup related implementations
void TaskGroup::schedule(Task task) {
   tasks.push_back(task);
}

struct JoinRunner {
   Task fn;
   Task* joinTask;
   awfy::atomic<size_t>* semaphore;

   JoinRunner(Task fn, Task* joinTask, awfy::atomic<size_t>* semaphore) : fn(fn), joinTask(joinTask), semaphore(semaphore) {
   }

   static void* run(JoinRunner* runner) {
      runner->fn.execute();
      auto status=runner->semaphore->fetch_add(-1)-1;
      if(status==0) {
         runner->joinTask->execute();
         delete runner->semaphore;
         delete runner->joinTask;
         delete runner;
      }
      return nullptr;
   }
};

void TaskGroup::join(Task joinTask) {
   if(tasks.size()==0) {
      schedule(joinTask);
      return;
   }

   std::vector<Task> wrappedTasks;

   auto joinTaskPtr = new Task(joinTask);
   auto joinCounter = new awfy::atomic<size_t>(tasks.size());

   // Wrap existing functions to implement the join
   for (unsigned i = 0; i < tasks.size(); ++i) {
      JoinRunner* runner = new JoinRunner(tasks[i], joinTaskPtr, joinCounter);
      Task wrappedFn((void (*)(void*)) &(JoinRunner::run), runner, tasks[i].groupId);
      wrappedTasks.push_back(wrappedFn);
   }
   tasks = std::move(wrappedTasks);

}

std::vector<Task> TaskGroup::close() {
   return std::move(tasks);
}
