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
#include "../metrics.hpp"
#include "chrono.hpp"

namespace awfy {
   namespace counters {
      struct AllocationStats {
         size_t totalBytes;
         size_t totalAllocations;

         AllocationStats();
      };

      struct TaskCounters {
         awfy::chrono::TimeFrame timeFrame;
         size_t numAllocations;
         size_t allocatedMemory;
         size_t scheduledTasks;
         unsigned groupId;

         void start();
         void end();
      };

      class ThreadCounters {
      public:
         unsigned threadId;
         std::vector<TaskCounters*> taskCounters;
         std::vector<awfy::chrono::TimeFrame> stallTimes;

         ThreadCounters(const unsigned threadId);
         ~ThreadCounters();

         ThreadCounters(ThreadCounters&& other)
            : threadId(other.threadId), taskCounters(move(other.taskCounters)), stallTimes(move(other.stallTimes))
         { }
         ThreadCounters& operator=(ThreadCounters&& other) {
            this->threadId=other.threadId;
            this->taskCounters=move(other.taskCounters);
            this->stallTimes=move(other.stallTimes);
            return *this;
         }

         void startStalled();
         void endStalled();
         void startTask(unsigned groupId);
         void endTask();
         TaskCounters& currentTaskCounters();
         AllocationStats getAllocationStats();

         void initThread();
      };

      class ProgramCounters {
         std::vector<awfy::chrono::TimeFrame> emptyScheduler;
         std::vector<ThreadCounters> threadCounters;
         uint64_t nextThreadId;

      public:
         uint64_t scheduledTasks;

         ProgramCounters(unsigned numThreads);
         ~ProgramCounters();
         void startStalledScheduler();
         void endStalledScheduler();
         void countScheduledTask();
         ThreadCounters& getThreadCounters();
         AllocationStats getAllocationStats();
         void printStats();
      };

      struct CurrentThread {
         static __thread uint64_t id;
      };
   }
}