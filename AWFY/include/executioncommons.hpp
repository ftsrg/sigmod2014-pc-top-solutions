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

#include "queryfiles.hpp"
#include "runtime.hpp"
#include "concurrent/scheduler.hpp"

struct RunBatch {
   Scheduler& scheduler;
   ScheduleGraph& taskGraph;
   TaskGraph::Node taskId;
   runtime::QueryState& queryState;
   queryfiles::QueryBatch* batch;
   RunBatch(Scheduler& scheduler, ScheduleGraph& taskGraph, TaskGraph::Node taskId, runtime::QueryState& queryState, queryfiles::QueryBatch* batch)
   : scheduler(scheduler), taskGraph(taskGraph), taskId(taskId), queryState(queryState), batch(batch)
   { }
   void operator()() {
      queryState.getBatchRunner()->run(scheduler, taskGraph, taskId, batch);
   }
};

struct UpdateTask {
   ScheduleGraph& taskGraph;
   TaskGraph::Node taskId;
   UpdateTask(ScheduleGraph& taskGraph, TaskGraph::Node taskId)
      : taskGraph(taskGraph), taskId(taskId)
   { }
   void operator()() {
      taskGraph.updateTask(taskId, -1);
   }
};

void scheduleQueries(TaskGraph::Node taskId, uint32_t queryType, Scheduler& scheduler, ScheduleGraph& taskGraph, queryfiles::QueryBatcher& batches,
   runtime::QueryState& queryState, bool logScheduling) {
      // Schedule query tasks
      TaskGroup queryTasks;
      unsigned count=0;
      auto taskBatches=batches.getBatches(queryType);
      for(auto batchIter=taskBatches.begin(); batchIter!=taskBatches.end(); batchIter++) {
         queryfiles::QueryBatch* batch = *batchIter;
         assert(batch->queryType==queryType);

         queryTasks.schedule(LambdaRunner::createLambdaTask(RunBatch(scheduler, taskGraph, taskId, queryState, batch), taskId));
         count++;
      }

      queryTasks.join(LambdaRunner::createLambdaTask(UpdateTask(taskGraph, taskId),taskId));

      if(logScheduling) {
      	assert(batches.batchCounts[queryType]==count);
      	LOG_PRINT("[Queries] Schedule " << count << " of type: "<< queryType);
      }

      // Disable early close
      taskGraph.updateTask(taskId, 1);
      if(taskId==TaskGraph::Query1) {
         scheduler.schedule(queryTasks.close(), Priorities::LOW, false);
      } else {
         scheduler.schedule(queryTasks.close(), Priorities::CRITICAL, false);
      }
}

struct ScheduleQueries {
	TaskGraph::Node taskId;
	uint32_t queryType;
	Scheduler& scheduler;
	ScheduleGraph& taskGraph;
	queryfiles::QueryBatcher& batches;
   runtime::QueryState& queryState;
   bool logScheduling;

	ScheduleQueries(TaskGraph::Node taskId, uint32_t queryType, Scheduler& scheduler, ScheduleGraph& taskGraph, queryfiles::QueryBatcher& batches,
   runtime::QueryState& queryState, bool logScheduling)
   	: taskId(taskId), queryType(queryType), scheduler(scheduler), taskGraph(taskGraph), batches(batches), queryState(queryState), logScheduling(logScheduling)
   { }

   void operator()() {
      scheduleQueries(taskId, queryType, scheduler, taskGraph, batches, queryState, logScheduling);
   }
};

struct ParseBatches {
	queryfiles::QueryBatcher& batches;

	ParseBatches(queryfiles::QueryBatcher& batches)
   	: batches(batches)
   { }

   void operator()() {
   	batches.parse();
   }
};

struct CloseScheduler {
	Scheduler& scheduler;

	CloseScheduler(Scheduler& scheduler)
   	: scheduler(scheduler)
   { }

   void operator()() {
   	scheduler.setCloseOnEmpty();
   }
};

template<class ValidateAnswersT, class ParseBatchesT>
void initScheduleGraph(Scheduler& scheduler, ScheduleGraph& taskGraph, FileIndexes& fileIndexes, const std::string& dataPath, queryfiles::QueryBatcher& batches,
   runtime::QueryState& queryState, bool* excludes, ValidateAnswersT answerValidator) {

      fileIndexes.setupIndexTasks(scheduler, taskGraph, dataPath, batches.getUsedTags());
      taskGraph.addEdge(TaskGraph::Initialize, TaskGraph::QueryLoading);
      taskGraph.addEdge(TaskGraph::QueryLoading, TaskGraph::Query1);
      taskGraph.addEdge(TaskGraph::QueryLoading, TaskGraph::Query2);
      taskGraph.addEdge(TaskGraph::QueryLoading, TaskGraph::Query3);
      taskGraph.addEdge(TaskGraph::QueryLoading, TaskGraph::Query4);

      taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::QueryLoading, ParseBatchesT(batches, excludes));

      taskGraph.setTaskFn(Priorities::HYPER_CRITICAL, TaskGraph::Query1, ScheduleQueries(TaskGraph::Query1, 0, scheduler, taskGraph, batches, queryState, true));
      taskGraph.setTaskFn(Priorities::HYPER_CRITICAL, TaskGraph::Query2, ScheduleQueries(TaskGraph::Query2, 1, scheduler, taskGraph, batches, queryState, true));
      taskGraph.setTaskFn(Priorities::HYPER_CRITICAL, TaskGraph::Query3, ScheduleQueries(TaskGraph::Query3, 2, scheduler, taskGraph, batches, queryState, true));
      taskGraph.setTaskFn(Priorities::HYPER_CRITICAL, TaskGraph::Query4, ScheduleQueries(TaskGraph::Query4, 3, scheduler, taskGraph, batches, queryState, true));

      taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::ValidateAnswers, answerValidator);
      taskGraph.addEdge(TaskGraph::ValidateAnswers, TaskGraph::Finish);

      taskGraph.setTaskFn(Priorities::DEFAULT, TaskGraph::Finish, CloseScheduler(scheduler));

      taskGraph.updateTask(TaskGraph::Initialize, -1);

      if(!excludes[0]) {
         taskGraph.addEdge(TaskGraph::Query1, TaskGraph::ValidateAnswers);
         taskGraph.updateTask(TaskGraph::IndexQ1, -1);
      }
      if(!excludes[1]) {
         taskGraph.addEdge(TaskGraph::Query2, TaskGraph::ValidateAnswers);
         taskGraph.updateTask(TaskGraph::IndexQ2, -1);
      }
      if(!excludes[2]) {
         taskGraph.addEdge(TaskGraph::Query3, TaskGraph::ValidateAnswers);
         taskGraph.updateTask(TaskGraph::IndexQ3, -1);
      }
      if(!excludes[3]) {
         taskGraph.addEdge(TaskGraph::Query4, TaskGraph::ValidateAnswers);
         taskGraph.updateTask(TaskGraph::IndexQ4, -1);
      }
      if(!excludes[1]||!excludes[2]) {
         taskGraph.updateTask(TaskGraph::IndexQ2orQ3, -1);
      }
      if(!excludes[1]||!excludes[3]) {
         taskGraph.updateTask(TaskGraph::IndexQ2orQ4, -1);
      }
}

void executeTaskGraph(const unsigned hardwareThreads, Scheduler& scheduler, awfy::counters::ProgramCounters& counters, awfy::counters::ThreadCounters& threadCounts) {
      #ifndef SEQUENTIAL
      const unsigned numIOThreads=hardwareThreads/2;
      std::vector<awfy::Thread<Executor>> threads;
      for (unsigned i = 0; i < hardwareThreads-1; ++i) {
         Executor* executor = new Executor(counters.getThreadCounters(),scheduler,i,i<numIOThreads);
         threads.emplace_back(&Executor::start, executor);
      }
      #endif
      threadCounts.endTask();
      Executor executor(threadCounts,scheduler,hardwareThreads-1, false);
      executor.run();


      #ifndef SEQUENTIAL
      for(auto threadIter=threads.begin(); threadIter!=threads.end(); threadIter++) {
         (*threadIter).join();
      }
      #endif
}