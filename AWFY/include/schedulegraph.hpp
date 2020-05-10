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

#include <array>
#include <vector>
#include <string>
#include "boost/unordered_set.hpp"
#include <memory>
#include <mutex>
#include "concurrent/atomic.hpp"
#include "concurrent/scheduler.hpp"

using boost::unordered_set;

using namespace std;

struct TaskGraph {
   enum Node {
      Initialize=0,
      QueryLoading,
      // Parsing related entries
      IndexQ1,
      IndexQ2,
      IndexQ2orQ3,
      IndexQ2orQ4,
      IndexQ3,
      IndexQ4,
      PersonMapping, // NEW
      Tag, //9
      NamePlace, // 10
      TagInForums, // 11
      PersonGraph,
      PersonCommented,
      CommentCreatorMap,
      HasInterest,
      Birthday,
      PersonPlace,
      HasForum,
      InterestStatistics,

      // Query related entries
      Query1,
      Query2, // 19
      Query3,
      Query4,

      // Execution related entries
      QueryExec,
      ValidateAnswers,
      Finish,
      Unknown=999
   };

   static const size_t size = Finish+1;

   static std::string getName(TaskGraph::Node node) {
      switch(node) {
         case Initialize : return "Initialize";
         case QueryLoading : return "QueryLoading";
         case IndexQ1 : return "IndexQ1";
         case IndexQ2 : return "IndexQ2";
         case IndexQ3 : return "IndexQ3";
         case IndexQ4 : return "IndexQ4";
         case IndexQ2orQ3 : return "IndexQ2orQ3";
         case IndexQ2orQ4 : return "IndexQ2orQ4";
         case PersonMapping : return "PersonMapping";
         case Tag : return "Tag";
         case NamePlace : return "NamePlace";
         case TagInForums : return "TagInForums";
         case PersonGraph : return "PersonGraph";
         case PersonCommented : return "PersonCommented";
         case CommentCreatorMap : return "CommentCreatorMap";
         case HasInterest : return "HasInterest";
         case Birthday : return "Birthday";
         case PersonPlace : return "PersonPlace";
         case HasForum : return "HasForum";
         case Query1 : return "Query1";
         case Query2 : return "Query2";
         case Query3 : return "Query3";
         case Query4 : return "Query4";
         case QueryExec : return "QueryExec";
         case ValidateAnswers : return "ValidateAnswers";
         case Finish : return "Finish";
         case Unknown: return "Unknown";
         default: return "Default";
      }
   }
};

namespace std {
   template <>
   struct hash<TaskGraph::Node> {
      size_t operator ()(TaskGraph::Node value) const {
         return static_cast<size_t>(value);
      }
   };
}


/// Simple dependency graph class which allows modelling task dependencies
/// and execute the tasks exactly once when the dependencies are fulfilled.
class ScheduleGraph {
private:
   Scheduler& scheduler;
   
   array<awfy::atomic<int64_t>,TaskGraph::size> taskValues;
   array<Task*, TaskGraph::size> taskFunction;
   array<Priorities::Priority,TaskGraph::size> taskPriority;
   array<awfy::atomic<uint8_t>,TaskGraph::size> triggered;
   array<unordered_set<TaskGraph::Node>,TaskGraph::size> targets;
   array<unordered_set<TaskGraph::Node>,TaskGraph::size> sources;

   void runTask(TaskGraph::Node task);

public:
   ScheduleGraph(Scheduler& scheduler);
   ScheduleGraph(const ScheduleGraph&) = delete;
   ScheduleGraph(ScheduleGraph&&) = delete;
   void setTaskFn(Priorities::Priority priority, TaskGraph::Node node, std::function<void()>&& fn);
   void setTaskFn(Priorities::Priority priority, TaskGraph::Node node, Task task);
   void addEdge(TaskGraph::Node source, TaskGraph::Node node);
   void updateTask(TaskGraph::Node task, size_t delta);

   void eraseNotUsedEdges();
};

/// Utility class to make conversion from lambda to task easier
class LambdaRunner {
   std::function<void()> fn;

   LambdaRunner(std::function<void()>&& fn);
public:
   static Task createLambdaTask(std::function<void()>&& fn, TaskGraph::Node node);
   static void* run(LambdaRunner* runner);
};