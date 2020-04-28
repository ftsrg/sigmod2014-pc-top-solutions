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

#include <string>
#include "concurrent/atomic.hpp"
#include "indexes.hpp"
#include "queryfiles.hpp"
#include "metrics.hpp"
#include "../query1.hpp"
#include "../query2.hpp"
#include "../query3.hpp"
#include "../query4.hpp"
#include "util/log.hpp"
#include "concurrent/scheduler.hpp"
#include "schedulegraph.hpp"

namespace runtime {

   struct QueryState;

   class BatchRunner {
      QueryState& state;

      // Statistics
      static awfy::atomic<size_t> runnerId_;
      const string runnerId;
      const metrics::BlockStats<>::LogSensor sensor;
      uint64_t queryCount;
      uint64_t batchCount;

   public:
      BatchRunner(QueryState& state);
      ~BatchRunner();
      BatchRunner(const BatchRunner&) = delete;
      BatchRunner(BatchRunner&&) = delete;
      void run(Scheduler& scheduler, ScheduleGraph& taskGraph, TaskGraph::Node taskId, queryfiles::QueryBatch* currentBatch);
   };

   awfy::atomic<size_t> BatchRunner::runnerId_;

   struct QueryState {
      ScheduleGraph& taskGraph;
      Scheduler& scheduler;
      FileIndexes& indexes;

      QueryState(ScheduleGraph& taskGraph, Scheduler& scheduler, FileIndexes& indexes) :
         taskGraph(taskGraph), scheduler(scheduler), indexes(indexes) {
      }

      BatchRunner* getBatchRunner() {
         static __thread BatchRunner* batchRunner;
         if(batchRunner==nullptr) {
            batchRunner=new BatchRunner(*this);
         }
         return batchRunner;
      }

      Query1::QueryRunner* getQuery1Runner() {
         static __thread Query1::QueryRunner* queryRunner;
         if(queryRunner==nullptr) {
            assert(indexes.personGraph!=nullptr);
            queryRunner = new Query1::QueryRunner(indexes);
         }
         return queryRunner;
      }

      Query2::QueryRunner* getQuery2Runner() {
         static __thread Query2::QueryRunner* queryRunner;
         if(queryRunner==nullptr) {
            queryRunner=new Query2::QueryRunner(indexes);
         }
         return queryRunner;
      }

      Query3::QueryRunner* getQuery3Runner() {
         static __thread Query3::QueryRunner* queryRunner;
         if(queryRunner==nullptr) {
            queryRunner = new Query3::QueryRunner(indexes);
         }
         return queryRunner;
      }

      Query4::QueryRunner* getQuery4Runner() {
         static __thread Query4::QueryRunner* queryRunner;
         if(queryRunner==nullptr) {
            queryRunner = new Query4::QueryRunner(taskGraph, scheduler, indexes);
         }
         return queryRunner;
      }
   };

   struct BatchUpdateTask {
   ScheduleGraph& taskGraph;
   const TaskGraph::Node task;
   BatchUpdateTask(ScheduleGraph& taskGraph, TaskGraph::Node task)
      : taskGraph(taskGraph), task(task)
   { }
   void operator()() {
      taskGraph.updateTask(task, -1);
   }
   };


   BatchRunner::BatchRunner(QueryState& state) : state(state), 
   runnerId(string("queryRunner")+std::to_string(static_cast<unsigned long long>(runnerId_.fetch_add(1)))), sensor(runnerId),
   queryCount(0), batchCount(0) {
   }

   BatchRunner::~BatchRunner() {
      LOG_PRINT("["<<runnerId<<"]"<<" #Batches: "<<batchCount<<", #Queries: "<<queryCount);
   }

   void BatchRunner::run(Scheduler& scheduler, ScheduleGraph& taskGraph, TaskGraph::Node /*taskId*/, queryfiles::QueryBatch* currentBatch) {
   assert(currentBatch!=nullptr);
   assert(currentBatch->count>0);

   auto currentEntry=currentBatch->entries;
      if(currentEntry->ignore) { return; } // only have ot chick first as all are the same
      // Get query type
      const auto baseQueryPtr=currentEntry->getQuery();
      const auto baseQuery=reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(baseQueryPtr);
      const auto queryType=baseQuery->id;

      const char* dummyResultStr=(new string(""))->c_str();

      // Execute logic for the corresponding query type
      auto& personMapper = state.indexes.personMapper;
      if(queryType == queryfiles::QueryParser::Query1::QueryId) {
         auto query1Runner=state.getQuery1Runner();
         while(currentEntry!=currentBatch->end) {
            const auto queryPtr=currentEntry->getQuery();
            // Check that there are only queries of one type in a batch
            assert(queryType==reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(queryPtr)->id);

            assert(currentEntry->result==nullptr);
            queryfiles::QueryParser::Query1* query = reinterpret_cast<queryfiles::QueryParser::Query1*>(queryPtr);
            query->p1 = personMapper.map(query->p1);
            query->p2 = personMapper.map(query->p2);
            long long int result = query1Runner->query(query->p1,query->p2,query->x);
            currentEntry->result=((new string(move(to_string(result))))->c_str());
            assert(currentEntry->result!=nullptr);

            queryCount++;
            currentEntry=currentEntry->getNextEntry();
         }
      } else if(queryType == queryfiles::QueryParser::Query2::QueryId) {
         auto query2Runner=state.getQuery2Runner();
         while(currentEntry!=currentBatch->end) {
            const auto queryPtr=currentEntry->getQuery();
            // Check that there are only queries of one type in a batch
            assert(queryType==reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(queryPtr)->id);

            assert(currentEntry->result==nullptr);
            queryfiles::QueryParser::Query2* query = reinterpret_cast<queryfiles::QueryParser::Query2*>(queryPtr);
            auto result = query2Runner->query(query->k,query->year, query->month, query->day);
            currentEntry->result=(new string(result))->c_str();
            assert(currentEntry->result!=nullptr);

            queryCount++;
            currentEntry=currentEntry->getNextEntry();
         }
      } else if(queryType == queryfiles::QueryParser::Query3::QueryId) {
         auto query3Runner=state.getQuery3Runner();
         while(currentEntry!=currentBatch->end) {
            const auto queryPtr=currentEntry->getQuery();
            // Check that there are only queries of one type in a batch
            assert(queryType==reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(queryPtr)->id);

            assert(currentEntry->result==nullptr);
            queryfiles::QueryParser::Query3* query = reinterpret_cast<queryfiles::QueryParser::Query3*>(queryPtr);
            auto result = query3Runner->query(query->k, query->hops, query->getPlace());
            currentEntry->result=(new string(result))->c_str();
            assert(currentEntry->result!=nullptr);

            queryCount++;
            currentEntry=currentEntry->getNextEntry();
         }
      } else if(queryType == queryfiles::QueryParser::Query4::QueryId) {
         auto query4Runner=state.getQuery4Runner();
         while(currentEntry!=currentBatch->end) {
            const auto queryPtr=currentEntry->getQuery();
            // Check that there are only queries of one type in a batch
            assert(queryType==reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(queryPtr)->id);

            assert(currentEntry->result==nullptr);
            queryfiles::QueryParser::Query4* query = reinterpret_cast<queryfiles::QueryParser::Query4*>(queryPtr);
            currentEntry->result = dummyResultStr;

            auto tasks=query4Runner->query(query->k, query->getTag(), currentEntry->result);
            // Finish this query only after subtypes have finished
            tasks.join(LambdaRunner::createLambdaTask(BatchUpdateTask(taskGraph,TaskGraph::Query4),TaskGraph::Query4));
            // Only allow to continue after join has finished
            taskGraph.updateTask(TaskGraph::Query4, 1);
            scheduler.schedule(tasks.close(), Priorities::LOW, false);

            queryCount++;
            currentEntry=currentEntry->getNextEntry();
         }
      } else {
         FATAL_ERROR("Invalid query id "<<(unsigned) queryType);
      }

      batchCount++;
   }

}
