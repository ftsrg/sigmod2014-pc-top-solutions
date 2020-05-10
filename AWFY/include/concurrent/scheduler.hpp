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

#pragma once

#include <vector>
#include <queue>
#include <assert.h>
#include <pthread.h>
#include "../util/counters.hpp"

#include "mutex.hpp"

class Task {
   private:
   void (*functionPtr)(void*);
   void* arguments;

   public:
   unsigned groupId;

   Task(void (*functionPtr)(void*),void* arguments, unsigned groupId=999)
      : functionPtr(functionPtr),arguments(arguments),groupId(groupId) {
   }

   ~Task() {
   }

   void execute() {
      (*functionPtr)(arguments);
   }
};

struct Priorities {
   enum Priority {
      LOW=10,
      DEFAULT=11,
      NORMAL=30,
      URGENT=50,
      CRITICAL=70,
      HYPER_CRITICAL=80
   };
};

struct TaskOrder {
   Priorities::Priority priority;
   unsigned insertion;

   TaskOrder(const Priorities::Priority priority,const unsigned insertion)
      : priority(priority), insertion(insertion) { }
};

typedef std::pair<TaskOrder,Task*> OrderedTask;

struct TaskOrderCmp {
   bool operator() (OrderedTask const &a, OrderedTask const &b) {
      return a.first.priority < b.first.priority || ((a.first.priority == b.first.priority) && a.first.insertion > b.first.insertion);
   }
};

// Priority ordered scheduler
class Scheduler {
   std::priority_queue<OrderedTask, std::vector<OrderedTask>, TaskOrderCmp> ioTasks;
   std::priority_queue<OrderedTask, std::vector<OrderedTask>, TaskOrderCmp> workTasks;
   awfy::Mutex taskMutex;
   awfy::Condition taskCondition;
   volatile bool closeOnEmpty;
   volatile bool currentlyEmpty;
   volatile unsigned nextTaskId;

   awfy::counters::ProgramCounters& counters;

public:
   Scheduler(awfy::counters::ProgramCounters& counters);
   ~Scheduler();

   Scheduler(const Scheduler&) = delete;
   Scheduler(Scheduler&&) = delete;

   void schedule(const std::vector<Task>& funcs, Priorities::Priority priority=Priorities::DEFAULT, bool isIO=true);
   void schedule(const Task& task, Priorities::Priority priority=Priorities::DEFAULT, bool isIO=true);
   Task* getTask(bool preferIO=true);
   size_t size();
   void setCloseOnEmpty();
};

// Simple executor that will run the tasks until no more exist
struct Executor {
   const bool preferIO;
   const uint32_t coreId;
   Scheduler& scheduler;
   awfy::counters::ThreadCounters& counters;

   Executor(awfy::counters::ThreadCounters& counters, Scheduler& scheduler,
      uint32_t coreId, bool preferIO) : preferIO(preferIO), coreId(coreId),scheduler(scheduler), counters(counters) {
   }

   void run();

   static void* start(void* argument);
};

class TaskGroup {
   std::vector<Task> tasks;

public:
   void schedule(Task task);
   void join(Task joinTask);
   std::vector<Task> close();
};
