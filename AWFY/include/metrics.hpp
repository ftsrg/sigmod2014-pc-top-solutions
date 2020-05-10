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

#include <fstream>
#include <chrono>
#include <string>
#include <unistd.h>
#include "util/log.hpp"

namespace metrics {

   /// Data structure to hold information about the memory consumption
   struct MemoryStats {
      int64_t rss;
      int64_t shared;
      int64_t priv; 

      static inline std::string getOutputFields() {
         return "(rss, shared, private)";
      }
   };

   inline metrics::MemoryStats operator-(metrics::MemoryStats const& lhs, metrics::MemoryStats const& rhs) {
      metrics::MemoryStats stats;
      stats.rss=lhs.rss-rhs.rss;
      stats.shared=lhs.shared-rhs.shared;
      stats.priv=lhs.priv-rhs.priv;
      return stats;
   }

   /// Measure the time and stores it in the target
   template<class TimePeriod=std::chrono::milliseconds>
   struct Timer {
      std::chrono::time_point<std::chrono::system_clock> start;
      uint64_t* target;

      Timer(uint64_t* target): 
            start(std::chrono::system_clock::now()), target(target) {
      }

      ~Timer() {
         auto end = std::chrono::system_clock::now();
         target[0] = std::chrono::duration_cast<TimePeriod>(end-start).count();
      }
   };

   /// Measures the consumed memory and stores it in the target
   struct MemorySensor {
      const MemoryStats start;
      MemoryStats* target;

      MemorySensor(MemoryStats* target) : start(measure()), target(target) {
      }

      ~MemorySensor() {
         MemoryStats end = measure();
         target[0]=end-start;
      }

      static MemoryStats measure() {
         MemoryStats stats;
         int tSize = 0, resident = 0, share = 0;
         std::ifstream buffer("/proc/self/statm");
         buffer >> tSize >> resident >> share;
         buffer.close();

         long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
         stats.rss = resident * page_size_kb ;
         stats.shared = share * page_size_kb;
         stats.priv = stats.rss - stats.shared;
         return stats;
      }
   };

   /// Holds the aggregated information for a execution block
   template<bool printStats=true, class TimePeriod=std::chrono::milliseconds>
   class BlockStats {
      struct Sensor {
         Timer<TimePeriod> timer;
         MemorySensor memory;

         Sensor(uint64_t* timeTarget, MemoryStats* memoryTarget) :
               timer(timeTarget), memory(memoryTarget) {
         }
      };

   public:
      class LogSensor {
         uint64_t duration;
         MemoryStats memory;
         std::string name;
         volatile Sensor* sensor;

      public:
         LogSensor(std::string name) : name(name)  {
            sensor=new Sensor(&duration, &memory);
         }

         ~LogSensor() {
            delete sensor;
            LOG_PRINT("["<<name<<"]"<<" Time: "<<duration);
            LOG_PRINT("["<<name<<"]"<<" Memory " <<MemoryStats::getOutputFields()<<": "<<memory.rss<<", "<<memory.shared<<", "<<memory.priv);
         }
      };

      uint64_t duration;
      MemoryStats memory;
      std::string name;

      BlockStats(std::string name="") : name(name) {
      }

      ~BlockStats() {
         if(printStats) {
            LOG_PRINT("["<<name<<"]"<<" Time: "<<duration);
            LOG_PRINT("["<<name<<"]"<<" Memory " <<MemoryStats::getOutputFields()<<": "<<memory.rss<<", "<<memory.shared<<", "<<memory.priv);
         }
      }

      Sensor blockSensor() {
         return move(Sensor(&duration, &memory));
      }
   };
}

inline std::ostream& operator<< (std::ostream& os, const metrics::MemoryStats& stats) {
   os << stats.rss << ", " << stats.shared << ", " << stats.priv;
   return os;
}
