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

#define Q4_BUILD_SUBGRAPH

#include <string>
#include "include/indexes.hpp"
#include "include/queue.hpp"
#include "include/subgraph.hpp"
#include "include/topklist.hpp"

using namespace std;

namespace Query4 {

struct BFSResult {
	uint64_t totalDistances;
	uint32_t totalReachable;
   bool earlyExit;
};

struct CentralityResult {
   PersonId person;
   uint64_t distances;
   uint32_t numReachable;
   double centrality;

   CentralityResult(PersonId person, uint64_t distances, uint32_t numReachable, double centrality)
      : person(person), distances(distances), numReachable(numReachable), centrality(centrality) {
   }

   bool operator==(const CentralityResult& other) const {
      return person==other.person&&centrality==other.centrality;
   }

   friend std::ostream& operator<<(std::ostream &out, const CentralityResult& v) {
      out<<v.person<<"::"<<v.distances;
      return out;
   }
};

struct PathInfo {
   PersonId person;
   uint32_t distance;

   PathInfo() : person(std::numeric_limits<PersonId>::max()), distance(std::numeric_limits<uint64_t>::max()) {

   }

   PathInfo(PersonId person, uint32_t distance) : person(person), distance(distance) {
   }
};

struct SearchState {
   campers::HashMap<PersonId,PathInfo> seen;
   awfy::Queue<pair<PersonId,uint32_t>> fringe;
   PersonId target;

   SearchState() : seen(128*1024), fringe(128*1024), target(0) {
   }

   void init(PersonId source, PersonId target) {
      seen.clear();
      fringe.clear();
      this->target=target;
      auto& p = fringe.push_back_pos();
      p.first=source;
      p.second=0;
      seen.tryInsert(source)[0]=PathInfo(source, 0);
   }
};

struct BidirectSearchState {
   array<SearchState,2> states;
};

typedef std::pair<PersonId, CentralityResult> CentralityEntry;

class QueryRunner; //Forward declaration

struct ConnectedComponentStats {
   vector<uint32_t> personComponents;
   vector<uint32_t> componentSizes;
   uint32_t maxComponentSize;
};

struct PersonEstimates {
   array<uint32_t,12> reachable;
   uint64_t distances;
   PersonId person;
   bool interesting;

   PersonEstimates() : reachable(), distances(0), person(0), interesting(false) {
   }

   #ifdef DEBUG
   void validate(uint32_t max, const std::string& loc) const {

      for(unsigned i=1; i<reachable.size(); i++) {
         if((i==1&&reachable[0]==0) ||
            (reachable[i]!=0 && reachable[i]<reachable[i-1]) ||
            (i>1&&reachable[i-1]==0&&reachable[i]>0)) {
            LOG_PRINT("Invalid person (reachable="<<max<<")"<<" from "<<loc);
            print();
            assert(false);
         }
      }
   }
   #else
   void validate(uint32_t /*max*/, const std::string& /*loc*/) const {
   }
   #endif

   void normalize(uint32_t max) {
      bool reachedMax=false;
      for(unsigned i=1; i<reachable.size(); i++) {
         if(reachedMax) {
            reachable[i]=max;
            continue;
         }

         // Normalize deltas
         if(reachable[i]<reachable[i-1]) {
            reachable[i]=max;
            LOG_PRINT("Conservative estimate");
         }

         if(reachable[i]>=max) {
            reachable[i]=max;
            reachedMax=true;
         }
      }
   }

   void print() const {
      LOG_PRINT("Person "<< person);
      for(unsigned i=0; i<reachable.size(); i++) {
         LOG_PRINT("Values "<<i<<": "<<reachable[i]);
      }
   }

   uint64_t calcDistanceBound(uint32_t alreadySeen, uint32_t totalReachable, const uint32_t startLevel) const {
      uint64_t distanceBound=0;
      uint32_t remaining=(totalReachable-1)-alreadySeen;
      uint32_t maxLevel=startLevel+1;
      assert(alreadySeen<=(totalReachable-1));

      // Build sum over estimates
      const uint32_t reachableSize=reachable.size();
      for(unsigned i=startLevel; i<reachableSize; i++) {
         if(reachable[i]==0) { break; }
         if(reachable[i]<alreadySeen) {
            LOG_PRINT("Underflow error: "<<startLevel<<"::"<<i<<"::"<<alreadySeen<<"::"<<remaining<<"::"<<distanceBound<<"::"<<totalReachable);
            print();
            validate(totalReachable-1, "BoundManager LBRD");
            LOG_PRINT("FAIL END");
         }
         assert(reachable[i]>=alreadySeen); // Underflow check
         auto delta=reachable[i]-alreadySeen;
         assert(remaining>=delta);
         distanceBound+=delta*(i+1);
         alreadySeen=reachable[i];
         remaining-=delta;
         maxLevel=i+1;
      }

      assert(startLevel>0 || (maxLevel==reachable.size() || maxLevel>=startLevel));
      if(maxLevel!=reachable.size()) {
         distanceBound+=remaining*maxLevel;
      } else {
         distanceBound+=remaining*(startLevel+1);
      }

      return distanceBound;
   }
};

struct PersonEstimatesData {
   vector<PersonId> orderedPersons;
   vector<PersonEstimates> personEstimates;
   const uint32_t estimationLevel;
   const uint32_t reachableIx;

   PersonEstimatesData(vector<PersonId> orderedPersons, vector<PersonEstimates> personEstimates, unsigned estimationLevel)
      : orderedPersons(move(orderedPersons)), personEstimates(move(personEstimates)), estimationLevel(estimationLevel), reachableIx(estimationLevel-2)
   { }

   static PersonEstimatesData create(const PersonSubgraph& subgraph, const ConnectedComponentStats& componentStats);
};

class QueryState {
public:
   QueryRunner& runner;

   // These are constant in the multithreaded part
   const uint32_t k;
   const uint32_t numPersonsInForums;
   PersonEstimatesData estimates;
   vector<uint8_t> personChecked; // Is used concurrently!!
   const PersonSubgraph subgraph;

   mutex topResultsMutex;
   awfy::TopKList<PersonId, CentralityResult> topResults;
   awfy::atomic<CentralityResult*> globalCentralityBound;
   uint32_t lastBoundUpdate;

   QueryState(QueryRunner& runner, uint32_t k, uint32_t numPersonsInForums, PersonEstimatesData estimates, PersonSubgraph subgraph, CentralityResult* globalCentralityBound)
      : runner(runner), k(k), numPersonsInForums(numPersonsInForums), estimates(move(estimates)), personChecked(subgraph.size()), subgraph(move(subgraph)),
         topResults(make_pair(globalCentralityBound->person,*globalCentralityBound)), globalCentralityBound(globalCentralityBound), lastBoundUpdate(0)
   {}
};

class QueryRunner {
private:
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
	const PersonGraph& knowsIndex;
	PersonMapper& personMapper;
	const TagIndex& tagIndex;
	const TagInForumsIndex& tagInForumsIndex;
	const HasMemberIndex& hasMemberIndex;

	uint32_t getDegree(PersonId person) const;

    pair<vector<char>,pair<uint32_t,uint64_t>> buildPersonFilter(InterestId tagId);
 
	void reset();

public:
	QueryRunner(ScheduleGraph& taskGraph, Scheduler& scheduler, FileIndexes& fileIndexes);
	TaskGroup query(const uint32_t k, const char* tag, const char*& resultOut); 
};

}
