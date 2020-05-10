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
#include <iomanip>
#include <sstream>
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

class QueryParamParser : public queryfiles::QueryParser 
{
   std::string stringParam;
   bool isRead;
public:
   BaseQuery* query;

   QueryParamParser(int argc, char **argv) {
      switch(argv[3][0]) {
         case Query1::QueryId: {
            auto query1 = new Query1();
            query1->id = Query1::QueryId;
            query1->p1 = std::stol(argv[4]);
            query1->p2 = std::stol(argv[5]);
            query1->x = std::stol(argv[6]);
            query = query1;
            break;
         }
         case Query2::QueryId: {
            auto query2 = new Query2();
            query2->id = Query2::QueryId;
            query2->k = std::stol(argv[4]);
            struct std::tm tm;
            std::istringstream ss(argv[5]);
            ss >> std::get_time(&tm, "%Y-%m-%d");
            query2->year = tm.tm_year + 1900;
            query2->month = tm.tm_mon + 1;
            query2->day = tm.tm_mday;
            query = query2;
            break;
         }
         case Query3::QueryId: {
            auto query3 = new Query3();
            query3->id = Query3::QueryId;
            query3->k = std::stol(argv[4]);
            query3->hops = std::stol(argv[5]);
            stringParam = argv[6];
            query = query3;
            break;
         }
         case Query4::QueryId: {
            auto query4 = new Query4();
            query4->id = Query4::QueryId;
            query4->k = std::stol(argv[4]);
            stringParam = argv[5];
            query = query4;
            break;
         }
         default:
            throw "invalid query number";
      }
   }

   ~QueryParamParser() override {
      delete query;
   }

   int64_t readNext(void* resultPtr) override {
      if (isRead) {
         return -1;
      }
      isRead = true;
      switch (query->id) {
         case Query1::QueryId:
            return copyQuery1(resultPtr);
         case Query2::QueryId:
            return copyQuery2(resultPtr);
         case Query3::QueryId:
            return copyQuery3(resultPtr);
         case Query4::QueryId:
            return copyQuery4(resultPtr);
         default:
            throw "not valid query id";
      }
   }
private:
   inline size_t copyQuery1(void* buffer) {
      std::memcpy(buffer, query, sizeof(Query1));
      return sizeof(Query1);
   }

   inline size_t copyQuery2(void* buffer) {
      std::memcpy(buffer, query, sizeof(Query2));
      return sizeof(Query2);
   }

   inline size_t copyQuery3(void* buffer) {
      std::memcpy(buffer, query, sizeof(Query3));

      auto strTarget = reinterpret_cast<char*>(buffer) + sizeof(Query3);
      for (unsigned i=0;i<stringParam.size();++i) // for loop often superior to memcpy
         *(strTarget+i)=stringParam[i];
      strTarget[stringParam.size()] = 0;

      return sizeof(Query3) + stringParam.size() + 1;
   }

   inline size_t copyQuery4(void* buffer) {
      std::memcpy(buffer, query, sizeof(Query4));

      auto strTarget = reinterpret_cast<char*>(buffer) + sizeof(Query4);
      for (unsigned i=0;i<stringParam.size();++i) // for loop often superior to memcpy
         *(strTarget+i)=stringParam[i];
      strTarget[stringParam.size()] = 0;

      return sizeof(Query4) + stringParam.size() + 1;
   }
};

const static std::string FILE_FLAG = "FILE";
const static std::string PARAM_FLAG = "PARAM";

int main(int argc, char **argv) {

   if(argc < 4) {
      cerr<<"Usage with query file: " << argv[0] << " <dataFolder> " << FILE_FLAG << " <queryFile>"<<endl;
      cerr<<"Usage with query params: " << argv[0] << " <dataFolder> " << PARAM_FLAG << " <queryNumber> <param1> <param2> ..."<<endl;
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
   const string dataPath(argv[1]);

   queryfiles::QueryParser* queries = nullptr;
   io::MmapedFile* queryFile = nullptr;
   if (argv[2] == FILE_FLAG) {
      const string queryPath(argv[argc-1]);

      queryFile = new io::MmapedFile(queryPath, O_RDONLY);
      queries = new queryfiles::QueryFileParser(*queryFile);
   } else if (argv[2] == PARAM_FLAG) {
      auto* paramParser = new QueryParamParser(argc, argv);
      auto queryIndex = queryfiles::QueryParser::getQueryIndex(paramParser->query->id);
      for (auto index = 0u; index < 4; ++index) {
         excludes[index] = index != queryIndex;
      }
      queries = paramParser;
   }

   queryfiles::QueryBatcher batches(*queries);
   
   FileIndexes fileIndexes;
   runtime::QueryState queryState(taskGraph, scheduler, fileIndexes);

   initScheduleGraph<PrintResults, ParseAllBatches>(scheduler, taskGraph, fileIndexes, dataPath, batches, queryState, excludes,
      PrintResults(counters, taskGraph, batches, fileIndexes, dataPath, start));

   taskGraph.eraseNotUsedEdges();

   executeTaskGraph(hardwareThreads, scheduler, counters, threadCounts);

   delete queries;
   delete queryFile;
   return 0;
}
