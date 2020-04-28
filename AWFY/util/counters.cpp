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

#include "../include/util/log.hpp"
#include <sys/types.h>
#include <assert.h>

#include "../include/util/counters.hpp"
#include "../include/util/memoryhooks.hpp"
#include "../include/schedulegraph.hpp"
#include "../include/compatibility.hpp"

namespace awfy {
   namespace counters {

__thread uint64_t CurrentThread::id=0;

//--- Memory hook functions
static volatile __thread TaskCounters* currentTask;
void countMemory(size_t size) {
   assert(DEBUG);
   assert(currentTask!=nullptr);
   currentTask->numAllocations++;
   currentTask->allocatedMemory+=size;
}

AllocationStats::AllocationStats() : totalBytes(0), totalAllocations(0) {

}

//--- TaskCounters methods
void TaskCounters::start() {
   timeFrame.start();
   allocatedMemory=0;
   scheduledTasks=0;
   #ifdef DEBUG
   assert(awfy::memoryhooks::CurrentThread::report_fn==nullptr);
   currentTask=this;
   awfy::memoryhooks::CurrentThread::report_fn=countMemory;
   #endif
}

void TaskCounters::end() {
   timeFrame.end();
   #ifdef DEBUG
   awfy::memoryhooks::CurrentThread::report_fn=nullptr;
   currentTask=nullptr;
   #endif
}

//--- ThreadCounters methods
ThreadCounters::ThreadCounters(const unsigned threadId) : threadId(threadId) {
}

ThreadCounters::~ThreadCounters() {
   #ifdef DEBUG
   for (unsigned i = 0; i < taskCounters.size(); ++i) {
      TaskCounters& counter=*taskCounters[i];
      awfy::chrono::TimeFrame& timeFrame = counter.timeFrame;
      #ifndef DEBUG_TRACE
         if(timeFrame.duration==0) { continue; }
         if(counter.scheduledTasks==0) { continue; }
      #endif
         
      #ifdef DEBUG_TRACE
      LOG_PRINT("[ThreadCounters] Thread "<<threadId<<" Task "<<TaskGraph::getName((TaskGraph::Node)counter.groupId)<<" "
         <<timeFrame.startTime<<" - "<<timeFrame.endTime<<" : "
         <<timeFrame.duration<<" ms running a task. "
         <<counter.scheduledTasks<<" tasks spawned. "
         <<counter.allocatedMemory/1024<<"kb allocated ("<<counter.numAllocations<<")."
         );
      #else
      LOG_PRINT("[ThreadCounters] Thread "<<threadId<<" Task "<<TaskGraph::getName((TaskGraph::Node)counter.groupId)<<" "
         <<timeFrame.startTime<<" - "<<timeFrame.endTime<<" : "
         <<timeFrame.duration<<" ms running a task. "
         <<counter.scheduledTasks<<" tasks spawned. "
         );
      #endif
   }

   for (unsigned i = 0; i < stallTimes.size(); ++i) {
      awfy::chrono::TimeFrame& timeFrame=stallTimes[i];
      #ifndef DEBUG_TRACE
         if(timeFrame.duration==0) { continue; }
      #endif
      LOG_PRINT("[ThreadCounters] Thread "<<threadId<<" "
         <<timeFrame.startTime<<" - "<<timeFrame.endTime<<" : "
         <<timeFrame.duration<<" ms waiting on tasks.");
   }
   #endif
}

void ThreadCounters::startStalled() {
   #ifdef DEBUG
   awfy::chrono::TimeFrame timeFrame;
   timeFrame.start();
   stallTimes.push_back(timeFrame);
   #endif
}

void ThreadCounters::endStalled() {
   #ifdef DEBUG
   stallTimes[stallTimes.size()-1].end();
   #endif
}

#ifdef DEBUG
void ThreadCounters::startTask(unsigned groupId) {
   TaskCounters* counters = new TaskCounters();
   counters->groupId=groupId;
   counters->start();
   taskCounters.push_back(counters);
}
#else
void ThreadCounters::startTask(unsigned) { }
#endif

void ThreadCounters::endTask() {
   #ifdef DEBUG
   taskCounters[taskCounters.size()-1]->end();
   #endif
}

TaskCounters& ThreadCounters::currentTaskCounters() {
   assert(DEBUG);
   return *taskCounters[taskCounters.size()-1];
}

AllocationStats ThreadCounters::getAllocationStats() {
   assert(DEBUG);
   AllocationStats stats;
   for (unsigned i = 0; i < taskCounters.size(); ++i) {
      TaskCounters& counter=*taskCounters[i];
      stats.totalBytes+=counter.allocatedMemory;
      stats.totalAllocations+=counter.numAllocations;
   }
   return stats;
}

void ThreadCounters::initThread() {
   CurrentThread::id=threadId;
}

//--- ProgramCounters methods
ProgramCounters::ProgramCounters(unsigned numThreads) : nextThreadId(0), scheduledTasks(0) {
   threadCounters.reserve(numThreads);
}

ProgramCounters::~ProgramCounters() {
   #ifdef DEBUG
   threadCounters.clear(); // Force destructor call of TaskCounters
   auto stats=getAllocationStats();
   uint64_t noWorkTime=0;
   for (unsigned i = 0; i < emptyScheduler.size(); ++i) {
      awfy::chrono::TimeFrame& timeFrame=emptyScheduler[i];
      noWorkTime+=timeFrame.duration;
      LOG_PRINT("[SchedulerCounts] "
         <<timeFrame.startTime<<" - "<<timeFrame.endTime<<" : "
         <<timeFrame.duration<<" ms no tasks.");
   }
   LOG_PRINT("[ProgramCounters] Total scheduled tasks: "<<scheduledTasks);
   LOG_PRINT("[ProgramCounters] Total allocated memory: "<<stats.totalBytes<<" in "<<stats.totalAllocations<<" allocations");
   #endif
}

void ProgramCounters::printStats() {
   #ifdef DEBUG
   uint64_t noWorkTime=0;
   std::vector<uint64_t> taskTimes;
   for (unsigned i = 0; i < threadCounters.size(); ++i) {
      auto& taskCounters=threadCounters[i].taskCounters;
      for (unsigned j = 0; j < taskCounters.size(); ++j) {
         auto& taskCount=*taskCounters[j];
         if(taskCount.groupId>=taskTimes.size()) { taskTimes.resize(taskCount.groupId+1);}
         taskTimes[taskCount.groupId]+=taskCount.timeFrame.duration;
      }
      auto& stallCounters=threadCounters[i].stallTimes;
      for (unsigned j = 0; j < stallCounters.size()-1; ++j) {
         noWorkTime+=stallCounters[j].duration;
      }
   }
   std::cerr<<"Stl:"<<noWorkTime<<"ms"<<std::endl;
   for (unsigned i = 0; i < taskTimes.size(); ++i) {
      if(taskTimes[i]>8*50) {
         std::cerr<<"T"<<i<<":"<<taskTimes[i]<<std::endl;
      }
   }
   #endif
}

ThreadCounters& ProgramCounters::getThreadCounters() {
   threadCounters.emplace_back(nextThreadId++);
   return threadCounters[threadCounters.size()-1];
}

void ProgramCounters::countScheduledTask() {
   #ifdef DEBUG
   assert(CurrentThread::id<threadCounters.size());
   threadCounters[CurrentThread::id].currentTaskCounters().scheduledTasks++;
   #endif
}

void ProgramCounters::startStalledScheduler() {
   #ifdef DEBUG
   awfy::chrono::TimeFrame timeFrame;
   timeFrame.start();
   emptyScheduler.push_back(timeFrame);
   #endif
}

void ProgramCounters::endStalledScheduler() {
   #ifdef DEBUG
   emptyScheduler[emptyScheduler.size()-1].end();
   if(emptyScheduler[emptyScheduler.size()-1].duration==0) {
      emptyScheduler.pop_back();
   }
   #endif
}

AllocationStats ProgramCounters::getAllocationStats() {
   assert(DEBUG);
   AllocationStats stats;
   for (unsigned i = 0; i < threadCounters.size(); ++i) {
      AllocationStats threadStats = threadCounters[i].getAllocationStats();
      stats.totalBytes+=threadStats.totalBytes;
      stats.totalAllocations+=threadStats.totalAllocations;
   }
   return stats;
}

}
}