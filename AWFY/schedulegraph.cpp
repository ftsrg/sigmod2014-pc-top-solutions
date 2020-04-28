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

#include "include/schedulegraph.hpp"

#include <assert.h>
#include <iostream>
#include "include/util/log.hpp"


void nodeFunctionExec(void* arguments) {
   auto fnPtr = static_cast<std::function<void()>*>(arguments);
   (*fnPtr)();
}

ScheduleGraph::ScheduleGraph(Scheduler& scheduler) : scheduler(scheduler) {
   for (unsigned i = 0; i < TaskGraph::size; ++i) {
      taskValues[i].store(1);
      taskFunction[i]=nullptr;
      triggered[i].store(0);
   }
}

void ScheduleGraph::updateTask(TaskGraph::Node task, size_t delta) {
   vector<TaskGraph::Node> triggeredTasks;
   {
      // Update dependency
      int64_t previous=taskValues[task].fetch_add(delta);
      int64_t current=previous+delta;
      assert(current>=0);
      if(current==0) {
         LOG_PRINT("[ScheduleGraph] Finished task node "<<TaskGraph::getName(task));
         // Check if edge can be activated
         auto& nextTasks = targets[task];
         for (auto itr = nextTasks.begin(); itr != nextTasks.end(); ++itr) {
            const auto& nextTask = *itr;
            if(!triggered[nextTask].load()) {
               bool trigger=true;
               auto& dependencies=sources[nextTask];
               for (auto dependency = dependencies.begin(); dependency != dependencies.end(); ++dependency) {
                  if(taskValues[*dependency].load() != 0) {
                     trigger=false;
                  }
               }

               // Compare and exchange will only work for one task
               if(trigger && !triggered[nextTask].fetch_or(1)) {
                  triggeredTasks.push_back(nextTask);
               }
            }
         }
      }
   }

   for (auto task = triggeredTasks.begin(); task != triggeredTasks.end(); ++task) {
      runTask(*task);
   }
}

void ScheduleGraph::setTaskFn(Priorities::Priority priority, TaskGraph::Node node, std::function<void()>&& fn) {
   auto funcPtr = new std::function<void()>(fn);
   Task task(nodeFunctionExec, funcPtr, node);
   setTaskFn(priority, node, task);
}

void ScheduleGraph::setTaskFn(Priorities::Priority priority, TaskGraph::Node node, Task task) {
   assert(task.groupId==(unsigned)node);
   taskFunction[node] = new Task(task);
   taskPriority[node] = priority;
}

void ScheduleGraph::addEdge(TaskGraph::Node source, TaskGraph::Node target) {
   bool found=false;
   auto& targetTasks=targets[source];
   for (auto task = targetTasks.begin(); task != targetTasks.end(); ++task) {
      if(*task==source) {
         found=true;
      }
   }
   assert(!found);

   targets[source].insert(target);
   sources[target].insert(source);
}

// Helper class
struct ScheduleGraphRunner {
   TaskGraph::Node node;
   Task* task;
   ScheduleGraph& graph;

   ScheduleGraphRunner(TaskGraph::Node node, Task* task, ScheduleGraph& graph) : node(node), task(task), graph(graph) {
   }

   static void* run(ScheduleGraphRunner* runner) {
      runner->task->execute();
      runner->graph.updateTask(runner->node, -1);
      delete runner->task;
      delete runner;
      return nullptr;
   }
};

template<class SchedulerRunner>
Task schedulerTask(SchedulerRunner* builder, TaskGraph::Node node) {
   return Task((void (*)(void*)) &(SchedulerRunner::run), builder, node);
}

void ScheduleGraph::runTask(TaskGraph::Node task) {
   LOG_PRINT("[ScheduleGraph] Scheduling task node "<<TaskGraph::getName(task));
   auto fn=taskFunction[task];
   taskFunction[task]=nullptr;

   ScheduleGraphRunner* taskRunner=new ScheduleGraphRunner(task, fn, *this);
   scheduler.schedule(schedulerTask(taskRunner,task), taskPriority[task]);
}

///--- LambdaRunner related methods
LambdaRunner::LambdaRunner(std::function<void()>&& fn) : fn(std::move(fn)) {
}

Task LambdaRunner::createLambdaTask(std::function<void()>&& fn, TaskGraph::Node node) {
   LambdaRunner* runner=new LambdaRunner(std::move(fn));
   return Task((void (*)(void*)) &(LambdaRunner::run), runner, node);
}

void* LambdaRunner::run(LambdaRunner* runner) {
   runner->fn();
   delete runner;
   return nullptr;
}
