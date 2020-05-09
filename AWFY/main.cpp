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

#include <chrono>
#include <memory>
#include <math.h>
#include "query1.hpp"
#include "query2.hpp"
#include "query3.hpp"
#include "query4.hpp"
#include "include/indexes.hpp"
#include "include/env.hpp"
#include "include/metrics.hpp"
#include "include/queryfiles.hpp"
#include "include/runtime.hpp"
#include "include/concurrent/scheduler.hpp"
#include "include/concurrent/thread.hpp"
#include "include/executioncommons.hpp"
#include "include/util/memoryhooks.hpp"

const uint32_t hardwareThreads=8; // Intel Xeon E5430 has 4 cores and no HT (2 sockets = 8 cores)

struct ParseAllBatches {
   queryfiles::QueryBatcher& batches;

   ParseAllBatches(queryfiles::QueryBatcher& batches, bool* /*unused*/)
      : batches(batches)
   { }

   void operator()() {
      batches.parse();
   }
};

struct PrintResults {
   awfy::counters::ProgramCounters& counters;
   ScheduleGraph& taskGraph;
   queryfiles::QueryBatcher& batches;
   FileIndexes& fileIndexes;
   const string& dataPath;
   awfy::chrono::Time start;
   PrintResults(awfy::counters::ProgramCounters& counters, ScheduleGraph& taskGraph, queryfiles::QueryBatcher& batches, FileIndexes& fileIndexes, const string& dataPath, awfy::chrono::Time start)
      : counters(counters), taskGraph(taskGraph), batches(batches), fileIndexes(fileIndexes), dataPath(dataPath), start(start)
   { }

   void operator()() {
      auto queryList=batches.getQueryList();
      counters.printStats();

      auto end=awfy::chrono::now();
      auto duration=end-start;

      cerr<<"DUR: "<<duration<<"ms, Busy: "<<(awfy::chrono::now()-end)<<" ms"<<endl;
      cerr<<"Q1:"<<batches.batchCounts[0]<<",Q2:"<<batches.batchCounts[1]<<",Q3:"<<batches.batchCounts[2]<<",Q4:"<<batches.batchCounts[3]<<endl;

      auto outputStart=awfy::chrono::now();
      // Print results
      for(auto queryIter=queryList.cbegin(); queryIter!=queryList.cend(); queryIter++) {
         // Compare results
         auto result = string((*queryIter)->result);
         cout<<result<<endl;
      }
      end=awfy::chrono::now();
      cerr<<"OUT:"<<end-outputStart<<" ms"<<endl;
      #ifdef DEBUG
      awfy::counters::AllocationStats stats=counters.getAllocationStats();
      cerr<<"MEM:"<<stats.totalBytes<<", "<<stats.totalAllocations<<endl;
      #endif
   }
};

static queryfiles::QueryParser* getQueryParser(int argc, char **argv) {
   const string queryPath(argv[argc-1]);

   io::MmapedFile queryFile(queryPath, O_RDONLY);
   auto queries = new queryfiles::QueryFileParser(queryFile);
   return queries;
}

int main(int argc, char **argv) {

   if(argc < 2) {
      cerr<<"Usage with query file: " << argv[0] <<" file <dataFolder> <queryFile>"<<endl;
      cerr<<"Usage with query params: " << argv[0] <<" param <dataFolder> <queryNumber> <param1> <param2> ..."<<endl;
      return -1;
   }

   srand (time(NULL));
   bool excludes[4] = {false, false, false, false};

   const auto start = awfy::chrono::now();
   awfy::counters::ProgramCounters counters(hardwareThreads);
   auto& threadCounts=counters.getThreadCounters();
   threadCounts.initThread();
   threadCounts.startTask(TaskGraph::Initialize);

   Scheduler scheduler(counters);
   ScheduleGraph taskGraph(scheduler);
   const string dataPath(argv[argc-2]);

   auto* queries = getQueryParser(argc, argv);

   queryfiles::QueryBatcher batches(*queries);
   
   FileIndexes fileIndexes;
   runtime::QueryState queryState(taskGraph, scheduler, fileIndexes);

   initScheduleGraph<PrintResults, ParseAllBatches>(scheduler, taskGraph, fileIndexes, dataPath, batches, queryState, excludes,
      PrintResults(counters, taskGraph, batches, fileIndexes, dataPath, start));

   taskGraph.eraseNotUsedEdges();

   executeTaskGraph(hardwareThreads, scheduler, counters, threadCounts);

   delete queries;
   return 0;
}
