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
#include "query1.hpp"
#include "query2.hpp"
#include "query3.hpp"
#include "query4.hpp"
#include "include/indexes.hpp"
#include "include/env.hpp"
#include "include/metrics.hpp"
#include "include/queryfiles.hpp"
#include "include/concurrent/scheduler.hpp"
#include "include/concurrent/thread.hpp"
#include "include/runtime.hpp"
#include "include/schedulegraph.hpp"
#include "include/executioncommons.hpp"
#include "include/util/log.hpp"
#include "include/util/memoryhooks.hpp"

const uint32_t hardwareThreads=8;

struct ParseBatchesFiletered {
   queryfiles::QueryBatcher& batches;
   bool* excludes;

   ParseBatchesFiletered(queryfiles::QueryBatcher& batches,bool excludes[4])
      : batches(batches), excludes(excludes)
   { }

   void operator()() {
      batches.parse();

      // Deactivate excluded queries
      auto queryList=batches.getQueryList();
      for(auto queryIter=queryList.begin(); queryIter!=queryList.end(); queryIter++) {
         auto& query = *queryIter;
         if(excludes[reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(query->getQuery())->id-'1']) {
            query->ignore=true;
         }
      }
   }
};

struct ValidateAnswers {
   ScheduleGraph& graph;
   const string& answerPath;
   queryfiles::QueryBatcher& batches;
   bool quickFail;
   size_t& failureCnt;
   size_t& successCnt;
   size_t& queryCnt;
   chrono::time_point<chrono::system_clock>& end;

   ValidateAnswers(ScheduleGraph& graph, const string& answerPath, queryfiles::QueryBatcher& batches, bool quickFail, 
      size_t& failureCnt, size_t& successCnt, size_t& queryCnt, chrono::time_point<chrono::system_clock>& end)
      : graph(graph), answerPath(answerPath), batches(batches), quickFail(quickFail), failureCnt(failureCnt),
         successCnt(successCnt), queryCnt(queryCnt), end(end)
   {

   }

   void operator()() {
      io::MmapedFile answerFile(answerPath, O_RDONLY);

      // Check results
      auto queryList=batches.getQueryList();
      queryfiles::AnswerParser answers(answerFile);

      for(auto queryIter=queryList.begin(); queryIter!=queryList.end(); queryIter++) {
         auto& query = *queryIter;
         // Skip excluded queries
         if(query->ignore) {
            answers.skipLine();
            continue;
         }

         // Compare results
         auto reference = answers.readAnswer();
         assert(query->result!=nullptr);
         auto result = string(query->result);
         if(result != reference) {
            failureCnt++;
            cerr<<"Error in line "<<queryCnt+1<<". Expected: "<<reference<<" got "<<result<<endl;
         } else {
            successCnt++;
         }

         if(quickFail&&failureCnt>0) {
            FATAL_ERROR("Wrong query answer");
         }
         queryCnt++;
      }

      end = chrono::system_clock::now();
   }
};

int main(int argc, char **argv) {
   if(argc < 4) {
      cerr<<"Usage [runTester] (-factor X) (-exclude q1,q2...) -F <dataFolder> <queryFile> <answerFile>"<<endl;
      return -1;
   }

   env::ArgsParser argsParser(argc, argv);
   auto excludesArgs = argsParser.getOption("-exclude");
   auto quickFail = argsParser.existsOption("-F");
   bool excludes[4] = {false, false, false, false};
   if(excludesArgs != nullptr) {
      const auto excludesStr = string(excludesArgs);
      if(excludesStr.find("1")!=std::string::npos) excludes[0] = true;
      if(excludesStr.find("2")!=std::string::npos) excludes[1] = true;
      if(excludesStr.find("3")!=std::string::npos) excludes[2] = true;
      if(excludesStr.find("4")!=std::string::npos) excludes[3] = true;
   }
   const auto workFactor = argsParser.getOptionAsUint32("-factor",1);

   const string dataPath(argv[argc-3]);
   const string queryPath(argv[argc-2]);
   const string answerPath(argv[argc-1]);

   io::MmapedFile queryFile(queryPath, O_RDONLY);
   FileIndexes fileIndexes;

   size_t failureCnt=0;
   size_t successCnt=0;
   size_t queryCnt=0;

   chrono::time_point<chrono::system_clock> start, end;
   auto initialMemory = metrics::MemorySensor::measure();
   start = chrono::system_clock::now();

   for(uint32_t i=0; i<workFactor; i++) {
      awfy::counters::ProgramCounters counters(hardwareThreads);
      auto& threadCounts=counters.getThreadCounters();
      threadCounts.initThread();
      threadCounts.startTask(TaskGraph::Initialize);

      Scheduler scheduler(counters);
      ScheduleGraph taskGraph(scheduler);

      queryfiles::QueryParser queries(queryFile);
      queryfiles::QueryBatcher batches(queries);
      runtime::QueryState queryState(taskGraph, scheduler, fileIndexes);

      initScheduleGraph<ValidateAnswers, ParseBatchesFiletered>(scheduler, taskGraph, fileIndexes, dataPath, batches, queryState, excludes,
         ValidateAnswers(taskGraph, answerPath, batches, quickFail, failureCnt, successCnt, queryCnt, end));

      executeTaskGraph(hardwareThreads, scheduler, counters, threadCounts);
   }

   auto endMemory = metrics::MemorySensor::measure();
   auto totalDuration = chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
   auto totalMemory = endMemory-initialMemory;

   cout<<"Tested "<<queryCnt<<" queries with "<<hardwareThreads<<" threads"<<endl;
   cout<<"(#Queries\tSuccess\t\tFailure\t\tIndex Time(ms)\tQuery Time(ms)\tTotal Time(ms))\tIndex Memory(kb)\tTotal Memory(kb)"<<endl;
   cout<<queryCnt<<"\t\t"<<successCnt<<"\t\t"<<failureCnt<<"\t\t"<<totalDuration/workFactor<<"\t\t"<<totalDuration<<"\t\t"<<totalDuration<<"\t\t"<<totalMemory<<"\t\t"<<totalMemory<<endl;

   if(failureCnt==0) {
      return 0;
   } else {
      return 1;
   }
}
