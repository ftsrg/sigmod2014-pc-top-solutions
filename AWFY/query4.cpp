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

#include <iostream>
#include <sstream>
#include <cmath>
#include <random>
#include "query4.hpp"
#include "include/alloc.hpp"

using namespace std;

// Comperator to create correct ordering of results
namespace awfy {
static const double EPSILON = 0.000000000001;
template<>
class TopKComparer<Query4::CentralityEntry> {
public:
   // Returns true if first param is larger or equal
   static bool compare(const Query4::CentralityEntry& a, const Query4::CentralityEntry& b)
   {
      auto delta = a.second.centrality - b.second.centrality;
      return ((delta >0) || (fabs(delta)< EPSILON && a.second.person < b.second.person));
   }
};
}

namespace Query4 {

typedef awfy::TopKComparer<Query4::CentralityEntry> CentralityCmp;

static const double MIN_CENTRALITY = 0.0;

// Creates a copy of the bound allocated on the heap
inline CentralityResult* makeHeapBound(const CentralityResult& centrality) {
   return new CentralityResult(centrality);
}

// Returns an initial bound that is larger than all possible bounds
inline CentralityResult* getInitialBound() {
   PersonId boundId = std::numeric_limits<PersonId>::max();
   return makeHeapBound(CentralityResult(boundId, 0, 0, MIN_CENTRALITY));
}

static const uint32_t morselSize = 128;
static const uint32_t maxMorselTasks = 128;
static const float boundsStablePercentage = 0.002; // Number of consecutive BFSs that must be prunable so that the state is considered stable
static const uint32_t minBoundRounds = 20;

typedef uint8_t Level;

QueryRunner::QueryRunner(ScheduleGraph& taskGraph, Scheduler& scheduler, FileIndexes& fileIndexes)
   : taskGraph(taskGraph), scheduler(scheduler), knowsIndex(*(fileIndexes.personGraph)),
     personMapper(fileIndexes.personMapper),
     tagIndex(*(fileIndexes.tagIndex)),
     tagInForumsIndex(*(fileIndexes.tagInForumsIndex.index)),
     hasMemberIndex(*(fileIndexes.hasMemberIndex))
{
   assert(fileIndexes.personGraph != nullptr);
   assert(fileIndexes.tagIndex != nullptr);
   assert(fileIndexes.tagInForumsIndex.index != nullptr);
   assert(fileIndexes.hasMemberIndex != nullptr);
}

void QueryRunner::reset() {
}

double getCloseness(uint32_t totalPersons,uint64_t totalDistances,uint32_t totalReachable) {
   return (totalDistances>0 && totalReachable>0 && totalPersons>0)
            ? static_cast<double>((totalReachable-1)*(totalReachable-1)) / (static_cast<double>((totalPersons-1))*totalDistances)
            : 0.0;
}

typedef pair<bool,uint64_t> DistanceBound;
inline DistanceBound getDistanceBound(const CentralityResult& bound, uint32_t numReachable, uint32_t numPersonsInForums) {
   bool checkBound=false;
   uint64_t localBound=std::numeric_limits<uint64_t>::max();
   assert(bound.centrality>=MIN_CENTRALITY);
   if(bound.centrality>MIN_CENTRALITY) {
      if(bound.numReachable==numReachable) {
         // Avoid floating point inaccuracies
         localBound=bound.distances;
      } else {
         double tmpBound=((numReachable-1)*(uint64_t)(numReachable-1))/(bound.centrality*(numPersonsInForums-1));
         localBound=tmpBound+1;
      }
      checkBound=true;
   }
   return make_pair(checkBound, localBound);
}

awfy::FixedSizeQueue<std::pair<PersonId, uint32_t>>& getThreadLocalToVisitQueue(size_t queueSize) {
   static __thread awfy::FixedSizeQueue<std::pair<PersonId, uint32_t>>* toVisitPtr=nullptr;
   if(toVisitPtr != nullptr) {
      awfy::FixedSizeQueue<std::pair<PersonId, uint32_t>>& q = *toVisitPtr;
      q.reset(queueSize);
      return q;
   } else {
      toVisitPtr = new awfy::FixedSizeQueue<std::pair<PersonId, uint32_t>>(queueSize);
      return *toVisitPtr;
   }
}

awfy::FixedSizeQueue<PersonId>& getThreadLocalPersonVisitQueue(size_t queueSize) {
   static __thread awfy::FixedSizeQueue<PersonId>* toVisitPtr=nullptr;
   if(toVisitPtr != nullptr) {
      awfy::FixedSizeQueue<PersonId>& q = *toVisitPtr;
      q.reset(queueSize);
      return q;
   } else {
      toVisitPtr = new awfy::FixedSizeQueue<PersonId>(queueSize);
      return *toVisitPtr;
   }
}

ConnectedComponentStats* calculateConnectedComponents(const PersonSubgraph& forumSubgraph) {
   assert(!forumSubgraph.personInSubgraph(0));

   const auto forumSubgraphSize = forumSubgraph.size();

   ConnectedComponentStats* stats = new ConnectedComponentStats();
   stats->personComponents.resize(forumSubgraphSize-1);
   stats->componentSizes.push_back(std::numeric_limits<uint32_t>::max()); // Component 0 is invalid

   awfy::FixedSizeQueue<std::pair<PersonId, uint32_t>>& toVisit = getThreadLocalToVisitQueue(forumSubgraph.size());
   assert(toVisit.empty()); //Data structures are in a sane state

   unsigned componentId=1;
   for(PersonId person=1; person<forumSubgraphSize; person++) {
      if(stats->personComponents[person-1]!=0) { continue; }

      uint32_t componentSize=1;
      stats->personComponents[person-1]=componentId;
      {
         auto& p = toVisit.push_back_pos();
         p.first=person;
      }
      do {
         const PersonId curPerson = toVisit.front().first;
         toVisit.pop_front();

         const auto curFriends=forumSubgraph.graph().retrieve(curPerson);
         assert(curFriends!=nullptr);

         auto friendsBounds = curFriends->bounds();
         while(friendsBounds.first != friendsBounds.second) {
            const PersonId curFriend=*friendsBounds.first;
            ++friendsBounds.first;
            if (stats->personComponents[curFriend-1]!=0) { continue; }
            stats->personComponents[curFriend-1]=componentId;
            componentSize++;
            auto& p = toVisit.push_back_pos();
            p.first = curFriend;
         }
      } while(!toVisit.empty());
      stats->componentSizes.push_back(componentSize);
      componentId++;
      LOG_PRINT("[Query4] Found component of size "<< componentSize);
   }

   uint32_t maxComponentSize=0;
   const uint32_t numComponents = stats->componentSizes.size();
   for(uint32_t component=1; component<numComponents; component++) {
      if(stats->componentSizes[component]>maxComponentSize) {
         maxComponentSize=stats->componentSizes[component];
      }
   }
   stats->maxComponentSize=maxComponentSize;
   LOG_PRINT("[Query4] Max component size "<< stats->maxComponentSize);
   LOG_PRINT("[Query4] Found number components "<< componentId-1);

   return stats;
}

uint8_t decideBFSVariant(QueryState& /*state*/, bool /*unstableBounds*/, const PersonId /*person*/, const PersonSubgraph& /*forumSubgraph*/) {
   return 0;
}

std::vector<PersonId> shortestPath(BidirectSearchState& searchState, const PersonSubgraph& forumSubgraph, PersonId p1, PersonId p2) {
   // Reset data structures and initialize for this search
   assert(searchState.states.size()==2);
   auto& bidiStates=searchState.states;
   bidiStates[0].init(p1, p2);
   bidiStates[1].init(p2, p1);

   // Run bidirectional search
   int8_t dir = 0;
   bool bidiJoined[2]={false,false};
   unsigned resultDist=std::numeric_limits<unsigned>::max();
   PersonId resultPerson=std::numeric_limits<unsigned>::max();
   while (!bidiStates[0].fringe.empty() && !bidiStates[1].fringe.empty()){
      dir=1-dir;

      auto& dirFringe=bidiStates[dir].fringe;
      auto& dirSeen=bidiStates[dir].seen;
      auto& dirTarget=bidiStates[dir].target;
      auto& otherDirSeen=bidiStates[1-dir].seen;

      // Fetch next person from queue
      PersonId curPerson = dirFringe.front().first;
      uint32_t curDepth = dirFringe.front().second;
      dirFringe.pop_front();

      // Check whether both bidirectional search met and thus finished
      if (bidiJoined[1-dir] && otherDirSeen.count(curPerson)) {
         assert(resultPerson!=std::numeric_limits<unsigned>::max());
         // Trace path back
         const PersonId otherTarget=bidiStates[1-dir].target;
         std::vector<PersonId> path;
         PersonId tracePerson=resultPerson;
         path.push_back(tracePerson);
         while(tracePerson!=otherTarget) {
            tracePerson=dirSeen.find(tracePerson)->person;
            path.push_back(tracePerson);
         }
         tracePerson=resultPerson;
         while(tracePerson!=dirTarget) {
            tracePerson=otherDirSeen.find(tracePerson)->person;
            path.push_back(tracePerson);
         }

         assert(path.size()==resultDist+1);
         return path;
      }

      // Load neighbours and comment information
      const auto curFriends=forumSubgraph.graph().retrieve(curPerson);
      assert(curFriends!=nullptr);

      auto friendsBounds = curFriends->bounds();
      while(friendsBounds.first != friendsBounds.second) {
         auto neighbourId = *friendsBounds.first;
         // Skip already seen neighbours
         if(dirSeen.count(neighbourId)) {
            friendsBounds.first++;
            continue;
         }

         auto neighbourDist=curDepth+1;

         // Return if we found the target
         if(neighbourId==dirTarget) {
            // Trace path back
            const PersonId otherTarget=bidiStates[1-dir].target;
            std::vector<PersonId> path;
            PersonId tracePerson=curPerson;
            path.push_back(neighbourId);
            path.push_back(curPerson);
            while(tracePerson!=otherTarget) {
               tracePerson=dirSeen.find(tracePerson)->person;
               path.push_back(tracePerson);
            }
            assert(path.size()==neighbourDist+1);
            return path;
         }

         // Insert neighbour distance information
         dirSeen.tryInsert(neighbourId)[0]=PathInfo(curPerson, neighbourDist);
         dirFringe.push_back(make_pair(neighbourId, neighbourDist));

         // Check whether the bidirectiony searches meet
         auto otherSeenNeighbour=otherDirSeen.find(neighbourId);
         if (otherSeenNeighbour!=nullptr){
            auto joinedDist=neighbourDist+otherSeenNeighbour->distance;
            if(resultDist>joinedDist) {
               resultPerson=neighbourId;
               resultDist=joinedDist; bidiJoined[dir]=true; 
            }
         }

         friendsBounds.first++;
      }
   }
   return std::vector<PersonId>();
}

struct BoundManager {
   PersonEstimates& estimate;
   uint64_t distances; // Exact distances
   uint64_t unknownBound;
   uint32_t reached;
   uint32_t totalReachable;

   BoundManager(PersonEstimates& estimate, const uint32_t totalReachable)
      : estimate(estimate), distances(0), unknownBound(0), reached(0), totalReachable(totalReachable) {

      unknownBound=estimate.calcDistanceBound(reached, totalReachable, 0);
   }

   BoundManager(BoundManager&& other)
      : estimate(other.estimate), distances(other.distances), unknownBound(other.unknownBound), reached(other.reached), totalReachable(other.totalReachable)
   { }

   BoundManager& operator=(BoundManager&& other) {
      this->estimate = other.estimate;
      this->distances = other.distances;
      this->unknownBound = other.unknownBound;
      this->reached = other.reached;
      this->totalReachable = other.totalReachable;
      return *this;
   }

   void updateDistEstimate(const uint32_t newReached, const uint32_t distance) {
      assert(newReached<=(totalReachable-1));

      if(distance>0) {
         auto delta=newReached-reached;
         distances+=delta*distance;
         reached+=delta;

         // Update estimate
         if((distance-1)<estimate.reachable.size()) {
            estimate.reachable[distance-1]=newReached;
         }

         unknownBound=estimate.calcDistanceBound(reached, totalReachable, distance);

         estimate.validate((totalReachable-1), "updateEstimate");
      }
   }

   void earlyExit(const uint32_t distance) {
            estimate.validate((totalReachable-1), "earlyExit A");

      if((distance-1)<estimate.reachable.size()) {
         if(!(distance<estimate.reachable.size() && estimate.reachable[distance]>0)) {
            estimate.reachable[distance-1]=(totalReachable-1);
         }
      }

      estimate.validate((totalReachable-1), "earlyExit B");
   }

   uint64_t getLowerDistanceBound() const {
      return distances+unknownBound;
   }
};

struct BatchBFSdata {
   PersonId person;
   uint32_t componentSize;
   BoundManager bfsBound;
   DistanceBound accurateDistanceBound;

   uint64_t totalDistances;
   uint32_t totalReachable;
   bool earlyExit;

   BatchBFSdata(PersonId person, uint32_t componentSize, BoundManager bfsBound, DistanceBound accurateDistanceBound)
      : person(person), componentSize(componentSize), bfsBound(move(bfsBound)), accurateDistanceBound(move(accurateDistanceBound)),
        totalDistances(0),totalReachable(0),earlyExit(false)
   { }

   BatchBFSdata(const BatchBFSdata&) = delete;
   BatchBFSdata& operator=(const BatchBFSdata&) = delete;

   BatchBFSdata(BatchBFSdata&& other)
      : person(other.person), componentSize(other.componentSize), bfsBound(move(other.bfsBound)), accurateDistanceBound(move(other.accurateDistanceBound)),
        totalDistances(other.totalDistances), totalReachable(other.totalReachable), earlyExit(other.earlyExit)
   { }
   BatchBFSdata& operator=(BatchBFSdata&& other) {
      this->person = other.person;
      this->componentSize = other.componentSize;
      this->bfsBound = move(other.bfsBound);
      this->accurateDistanceBound = move(other.accurateDistanceBound);
      return *this;
   }
};

struct BFSRunner {
   struct BFSState {
      const uint64_t localBound;
      const uint32_t numTotalReachable;
      const bool checkBound;

      BFSResult result;
      BoundManager& bfsBound;

      BFSState(const DistanceBound distanceBound, BoundManager& bfsBound, const uint32_t numTotalReachable)
            : localBound(distanceBound.second), numTotalReachable(numTotalReachable), checkBound(distanceBound.first), bfsBound(bfsBound) {
         result.totalReachable=0;
         result.totalDistances=0;
         result.earlyExit=false;
      }
   };

   typedef awfy::FixedSizeQueue<PersonId> BFSQueue;

   static BFSResult __attribute__ ((noinline)) run(const PersonId start, const PersonSubgraph& subgraph, const DistanceBound distanceBound, BoundManager& bfsBound, const uint32_t numTotalReachable) {
      BFSState state(distanceBound, bfsBound, numTotalReachable);

      BFSQueue& toVisit = getThreadLocalPersonVisitQueue(subgraph.size());
      assert(toVisit.empty()); //Data structures are in a sane state

      // Initialize BFS
      Level* seen = new Level[subgraph.size()]();
      seen[start] = 1; // Level = distance + 1, Level 0 = not seen
      {
         auto& p = toVisit.push_back_pos();
         p=start;
      }

      // Run rounds until we can either early exit or have reached all nodes
      uint32_t distance=0;
      do {
         const uint32_t personsRemaining=(state.numTotalReachable-1)-state.result.totalReachable;
         uint32_t numDiscovered = runRound(subgraph, seen, toVisit, toVisit.size(), personsRemaining);
         distance++;

         // Adjust result
         state.result.totalReachable+=numDiscovered;
         state.result.totalDistances+=numDiscovered*distance;

         // Update unseen bound estimates
         bfsBound.updateDistEstimate(state.result.totalReachable, distance);
         assert(state.bfsBound.distances==state.result.totalDistances);

         // Exit criteria for full BFS
         if((numTotalReachable-1)==state.result.totalReachable) {
            break;
         }

         // Update estimate
         if(state.checkBound && state.bfsBound.getLowerDistanceBound()>state.localBound) {
            state.bfsBound.earlyExit(distance+1);
            state.result.earlyExit=true;
            break;
         }
      } while(true);

      delete[] seen;
      return state.result;
   }

   private:

   static uint32_t __attribute__((hot)) runRound(const PersonSubgraph& subgraph, Level* __restrict__ seen, BFSQueue& toVisit, const uint32_t numToVisit, const uint32_t numUnseen) {
      uint32_t numRemainingToVisit=numToVisit;
      uint32_t numRemainingUnseen=numUnseen;

      do {
         const PersonId person = toVisit.front();
         toVisit.pop_front();

         // Iterate over friends
         const auto& friends=*subgraph.graph().retrieve(person);
         auto friendsBounds = friends.bounds();
         while(friendsBounds.first != friendsBounds.second) {
            assert(*friendsBounds.first<subgraph.size());
            subgraph.assertInSubgraph(*friendsBounds.first);
            if (likely(seen[*friendsBounds.first])) {
               ++friendsBounds.first;
               continue;
            }
            auto& p = toVisit.push_back_pos();
            p = *friendsBounds.first;

            seen[*friendsBounds.first] = true;
            ++friendsBounds.first;
            numRemainingUnseen--;
         }

         assert(!toVisit.empty());
         numRemainingToVisit--;
      } while(numRemainingToVisit>0 && numRemainingUnseen>0);

      return numUnseen-numRemainingUnseen;
   }


   struct BatchToVisitInfo {
      uint64_t queries;
      PersonId person;
      BatchToVisitInfo() : queries(0)
      { }
      BatchToVisitInfo(uint64_t queries, PersonId person)
         : queries(queries), person(person)
      { }
   };
   struct BatchToVisitInfoComparer {
      bool operator()(const BatchToVisitInfo& t1, const BatchToVisitInfo& t2) {
         return t1.person>t2.person;
      }
   };

public:
   static void runBatch(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph) {
      const auto subgraphSize = subgraph.size();

      array<uint64_t*,2> toVisitLists;
      toVisitLists[0] = new uint64_t[subgraphSize];
      memset(toVisitLists[0],0,sizeof(uint64_t)*subgraphSize);
      toVisitLists[1] = new uint64_t[subgraphSize];
      memset(toVisitLists[1],0,sizeof(uint64_t)*subgraphSize);

      const uint32_t numQueries = bfsData.size();
      assert(numQueries>0 && numQueries<=64);

      PersonId minPerson = numeric_limits<PersonId>::max();
      uint64_t* seen = new uint64_t[subgraphSize];
      memset(seen, 0, sizeof(uint64_t)*subgraphSize);
      for(size_t a=0; a<numQueries; a++) {
         const uint64_t personMask = 1UL<<a;
         assert(seen[bfsData[a].person]==0);
         seen[bfsData[a].person] = personMask;
         (toVisitLists[0])[bfsData[a].person] = personMask;
         minPerson = min(minPerson, bfsData[a].person);
      }

      runBatchRound(bfsData, subgraph, minPerson, toVisitLists, seen);

      delete[] seen;
      delete[] toVisitLists[0];
      delete[] toVisitLists[1];
   }

   static void __attribute__((hot)) runBatchRound(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, array<uint64_t*,2>& toVisitLists, uint64_t* __restrict__ seen) {
      const auto subgraphSize = subgraph.size();
      const uint32_t numQueries = bfsData.size();

      uint64_t processQuery = (~0UL);
      uint32_t queriesToProcess=numQueries;

      uint32_t numDistDiscovered[64] __attribute__((aligned(16)));  
      memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);

      uint8_t curToVisitQueue = 0;
      uint32_t nextDistance = 1;

      PersonId curPerson=minPerson;
      bool nextToVisitEmpty = true;
      do {
         const uint64_t* const toVisit = toVisitLists[curToVisitQueue];
         uint64_t* const nextToVisit = toVisitLists[1-curToVisitQueue];

         for(; curPerson<subgraphSize && toVisit[curPerson]==0; curPerson++) { }
         if(likely(curPerson<subgraphSize)) {
            const uint64_t toVisitEntry = toVisit[curPerson];

            const auto& curFriends=*subgraph.graph().retrieve(curPerson);
            assert(subgraph.graph().retrieve(curPerson)!=nullptr);

            const auto firstQueryId = __builtin_ctzl(toVisitEntry);
            if((toVisitEntry>>(firstQueryId+1)) == 0) {
               //Only single person in this entry

               auto friendsBounds = curFriends.bounds();
               while(friendsBounds.first != friendsBounds.second) {
                  if(toVisitEntry & processQuery & (~seen[*friendsBounds.first])) {
                     seen[*friendsBounds.first] |= toVisitEntry;
                     nextToVisit[*friendsBounds.first] |= toVisitEntry;
                     nextToVisitEmpty = false;
                     numDistDiscovered[firstQueryId]++;
                  }
                  ++friendsBounds.first;
               }
            } else {
               auto friendsBounds = curFriends.bounds();
               while(friendsBounds.first != friendsBounds.second) {

                  uint64_t newToVisit = toVisitEntry & processQuery & (~seen[*friendsBounds.first]); //!seen & toVisit
                  if(newToVisit == 0) {
                     ++friendsBounds.first;
                     continue;
                  }

                  seen[*friendsBounds.first] |= toVisitEntry;
                  nextToVisit[*friendsBounds.first] |= newToVisit;
                  
                  nextToVisitEmpty = false;

                   uint32_t pos=0;
                   do {
                      const auto numTZ = __builtin_ctzl(newToVisit);
                      numDistDiscovered[pos+numTZ]++;
                      if(unlikely(numTZ==63)) {
                         break;
                      }
                      pos += numTZ+1;
                      newToVisit = newToVisit>>(numTZ+1);
                   } while(newToVisit>0);

                  ++friendsBounds.first;
               }
            }

            //Go to next person
            curPerson++;
         } else {
            //Swap queues
            for(uint32_t a=0; a<numQueries; a++) {
               if(likely(processQuery & (1UL<<a))) {
                  bfsData[a].totalReachable += numDistDiscovered[a];
                  bfsData[a].totalDistances += numDistDiscovered[a]*nextDistance;

                  bfsData[a].bfsBound.updateDistEstimate(bfsData[a].totalReachable, nextDistance);
                  assert(bfsData[a].bfsBound.distances==bfsData[a].totalDistances);

                  if(unlikely((bfsData[a].componentSize-1)==bfsData[a].totalReachable)) {
                     if(queriesToProcess==1) {
                        return;
                     }
                     processQuery &= ~(1UL<<a);
                     queriesToProcess--;
                     continue;
                  }

                  // Update estimate
                  if(bfsData[a].accurateDistanceBound.first && bfsData[a].bfsBound.getLowerDistanceBound()>bfsData[a].accurateDistanceBound.second) {
                     bfsData[a].bfsBound.earlyExit(nextDistance+1);
                     bfsData[a].earlyExit=true;
                     
                     if(unlikely(queriesToProcess==1)) {
                        return;
                     }
                     processQuery &= ~(1UL<<a);
                     queriesToProcess--;
                  }
               }
            }
            if(unlikely(nextToVisitEmpty)) {
               return;
            }

            memset(toVisitLists[curToVisitQueue],0,sizeof(uint64_t)*subgraphSize);
            memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);
            nextToVisitEmpty = true;

            curPerson = 0;
            nextDistance++;
            curToVisitQueue = 1-curToVisitQueue;
         }
      } while(true);
   }
};

struct PruningStats {
   awfy::atomic<uint32_t> numEarlyPruning;
   awfy::atomic<uint64_t> numReachedPerson;
   awfy::atomic<uint32_t> numEarlyBfsExists;
   awfy::atomic<uint32_t> numBoundImprovements;
   awfy::atomic<uint32_t> numBoundImprovementsAfterInit;
   awfy::atomic<uint32_t> numNeighbourPruningBfs;

   PruningStats() : numEarlyPruning(0), numReachedPerson(0), numEarlyBfsExists(0), numBoundImprovements(0), numBoundImprovementsAfterInit(0) {
   }
};

struct EstimateComparer {
   const vector<PersonEstimates>& personEstimates;

   EstimateComparer(const vector<PersonEstimates>& personEstimates)
      : personEstimates(personEstimates) {
   }

   bool operator()(const PersonId& a, const PersonId& b) {
      const auto distA=personEstimates[a].distances>>4;
      const auto distB=personEstimates[b].distances>>4;
      return distA < distB || (distA == distB && a < b );
   }
};

PersonEstimatesData PersonEstimatesData::create(const PersonSubgraph& subgraph, const ConnectedComponentStats& componentStats) {
   const auto subgraphSize = subgraph.size();

   vector<PersonEstimates> personEstimates;
   personEstimates.resize(subgraphSize);
   vector<PersonId> orderedPersons;
   orderedPersons.reserve(subgraphSize);

   assert(!subgraph.personInSubgraph(0)); // Assume subgraph variant

   // Init reachable estimates for distance 1 and 2 for friends
   for (PersonId person = 1; person<subgraphSize; ++person) {
      assert(subgraph.personInSubgraph(person));
      const auto friends = subgraph.graph().retrieve(person);
      assert(friends!=nullptr);
      personEstimates[person].person=person;
      personEstimates[person].reachable[0]=friends->size();
      personEstimates[person].distances+=friends->size();
      orderedPersons.push_back(person);
   }

   const uint32_t maxDistance=personEstimates[0].reachable.size(); // +1 because distance 1 = index 0 in array
   uint32_t distIx=1;
   uint32_t prevDistIx=0;
   const uint32_t componentReachable=componentStats.maxComponentSize;

   bool reachedMax=false; //Time to end estimating?
   while(!reachedMax && distIx<maxDistance) {
      for (PersonId person = 1; person<subgraphSize; ++person) {
         assert(subgraph.personInSubgraph(person));

         // For new estimate sum up the estimates of dist-1 of the friends
         uint32_t countReachable=0;
         const auto friends = subgraph.graph().retrieve(person);
         auto friendsBounds = friends->bounds();
         while(friendsBounds.first != friendsBounds.second) {
            PersonId friendId=*friendsBounds.first;
            countReachable+=personEstimates[friendId].reachable[prevDistIx];
            ++friendsBounds.first;
         }

         // Correction for propagation error
         if(prevDistIx>=1) {
            countReachable-=personEstimates[person].reachable[prevDistIx-1]*(friends->size()-1);
         }

         // Sanity check for over estimation
         if(countReachable>=(componentReachable-1)) {
            countReachable=(componentReachable-1);
            reachedMax=true;
         }
         assert(countReachable>=personEstimates[person].reachable[prevDistIx]);

         // Update estimate
         personEstimates[person].reachable[distIx]=countReachable;
      }

      distIx++;
      prevDistIx++;
   }

   for (PersonId person = 1; person<subgraphSize; ++person) {
      if(distIx<maxDistance && personEstimates[person].reachable[prevDistIx]!=(componentReachable-1)) {
         personEstimates[person].reachable[distIx]=(componentReachable-1);
      }
      // Use correct connected componennt size at this point
      const auto personReachable=componentStats.componentSizes[componentStats.personComponents[person-1]];
      personEstimates[person].normalize(personReachable-1);
      personEstimates[person].distances=personEstimates[person].calcDistanceBound(0, personReachable, 0);
   }

   const unsigned estimationLevel = distIx+1;
   LOG_PRINT("[Query4] Using estimation level "<<estimationLevel);

   // Sort persons by degree
   sort(orderedPersons.begin(), orderedPersons.end(), EstimateComparer(personEstimates));

   return PersonEstimatesData(move(orderedPersons), move(personEstimates), estimationLevel);
}

void updatePersonEstimate(QueryState& state, const PersonId person, const uint32_t componentReachable) {
   assert((componentReachable-1)>0);

   PersonEstimates personEstimate;
   personEstimate.person=person;

   // Sum up estimates from friends
   const auto friends = state.subgraph.graph().retrieve(person);
   personEstimate.reachable[0] = friends->size();

   // Calculate new reachable per level
   std::pair<const PersonId*,const PersonId*> friendsBounds = friends->bounds();
   while(friendsBounds.first != friendsBounds.second) {
      PersonEstimates& friendEstimate = state.estimates.personEstimates[*friendsBounds.first];
      assert(friendEstimate.person==*friendsBounds.first);
      for(unsigned i=1; i<friendEstimate.reachable.size(); i++) {
         auto newReachable=personEstimate.reachable[i]+friendEstimate.reachable[i-1];
         // Cap reachable with max reachabel value
         if(newReachable>(componentReachable-1)) {
            newReachable=(componentReachable-1);
         }

         personEstimate.reachable[i]=newReachable;
      }
      ++friendsBounds.first;
   }

   personEstimate.normalize(componentReachable-1);
   personEstimate.validate(componentReachable-1, "updateEstimate A");

   // Calculate distance
   personEstimate.distances=personEstimate.calcDistanceBound(0, componentReachable, 0);

   // Store updated personEstimate
   state.estimates.personEstimates[person] = personEstimate;
}

struct MorselTask {
private:
   QueryState& state;
   const uint32_t rangeStart;
   const uint32_t rangeEnd;
   PruningStats& pruningStats;
   const bool abortOnceStable;
   uint32_t _lastOffset;
   ConnectedComponentStats& componentStats;

public:
   MorselTask(QueryState& state, uint32_t rangeStart, uint32_t rangeEnd, PruningStats& pruningStats, bool abortOnceStable /* Aborts processing once a first stable estimate state is reached */, ConnectedComponentStats& componentStats)
      : state(state), rangeStart(rangeStart), rangeEnd(rangeEnd), pruningStats(pruningStats), abortOnceStable(abortOnceStable), componentStats(componentStats)
   { }

   //Returns whether the bound was updated
   bool processSinglePerson(uint32_t rangeOffset, PersonId subgraphPersonId) {
      if(state.personChecked[subgraphPersonId]) {
         LOG_PRINT("FAIL ALREADY SEARCHED: "<<rangeOffset);
      }
      assert(!state.personChecked[subgraphPersonId]);
      assert(state.subgraph.personInSubgraph(subgraphPersonId));
      const CentralityResult centralityBound=*state.globalCentralityBound.load();
      const auto componentReachable=componentStats.componentSizes[componentStats.personComponents[subgraphPersonId-1]];

      updatePersonEstimate(state, subgraphPersonId, componentReachable);
      PersonEstimates& estimate = state.estimates.personEstimates[subgraphPersonId];

      // Try to exit early with the approximation
      BoundManager bfsBound(estimate, componentReachable);
      auto accurateDistanceBound=getDistanceBound(centralityBound, componentReachable, state.numPersonsInForums);
      const bool checkBound=accurateDistanceBound.first;
      if(checkBound) {
         const uint64_t localBound=accurateDistanceBound.second;
         const uint64_t estimatedCost=bfsBound.getLowerDistanceBound();
         if(estimatedCost>localBound) {
            pruningStats.numEarlyPruning.fetch_add(1);
            state.personChecked[subgraphPersonId]=true;
            return false;
         }
      }
      state.personChecked[subgraphPersonId] = true;

      // Run actual BFS
      const auto bfsResult=BFSRunner::run(subgraphPersonId, state.subgraph, accurateDistanceBound, bfsBound, componentReachable);
      estimate.validate(componentReachable, "after BFS");
      const auto closeness = getCloseness(state.numPersonsInForums, bfsResult.totalDistances, bfsResult.totalReachable);
      const PersonId externalPersonId = state.subgraph.mapFromSubgraph(subgraphPersonId);
      CentralityResult resultCentrality(externalPersonId, bfsResult.totalDistances, bfsResult.totalReachable, closeness);
      pruningStats.numReachedPerson.fetch_add(bfsResult.totalReachable);

      // Check if person qualifies as new top k value
      bool boundUpdated=false;
      if(!bfsResult.earlyExit) {
         if(CentralityCmp::compare(make_pair(resultCentrality.person, resultCentrality), make_pair(centralityBound.person, centralityBound))) {
            // Add improved value to top k list
            lock_guard<mutex> lock(state.topResultsMutex);
            state.topResults.insert(resultCentrality.person, resultCentrality);
            state.globalCentralityBound.store(makeHeapBound(state.topResults.getBound().second));
            CentralityResult updatedBound=*state.globalCentralityBound.load();
            // Check if person truely updated the global bound
            if(!(updatedBound==centralityBound)) {
               boundUpdated=true;
               LOG_PRINT("[BoundUpdate] Order position: "<<rangeOffset<<" of "<<state.numPersonsInForums);
               state.lastBoundUpdate=rangeOffset;
            }
         }
      } else {
         pruningStats.numEarlyBfsExists.fetch_add(1);
      }
      return boundUpdated;
   }

   //Returns pair of processed persons and whether the bound was updated
   pair<uint32_t,bool> processPersonBatch(const vector<PersonId>& persons, uint32_t begin, uint32_t end) {
      const CentralityResult centralityBound=*state.globalCentralityBound.load();

      //Build batch of up to 64 persons
      vector<BatchBFSdata> batchData;
      batchData.reserve(64);
      uint32_t p=begin;
      for(; batchData.size()<64 && p<end; p++) {
         const PersonId subgraphPersonId = persons[p];
         assert(!state.personChecked[subgraphPersonId]);
         assert(state.subgraph.personInSubgraph(subgraphPersonId));

         const uint32_t componentSize = componentStats.componentSizes[componentStats.personComponents[subgraphPersonId-1]];
         updatePersonEstimate(state, subgraphPersonId, componentSize);

         BatchBFSdata personData(subgraphPersonId, componentSize,
            BoundManager(state.estimates.personEstimates[subgraphPersonId], componentSize),
            getDistanceBound(centralityBound, componentSize, state.numPersonsInForums));

         // Try to exit early with the approximation
         const bool checkBound = personData.accurateDistanceBound.first;
         if(checkBound) {
            const uint64_t localBound = personData.accurateDistanceBound.second;
            const uint64_t estimatedCost = personData.bfsBound.getLowerDistanceBound();
            if(estimatedCost > localBound) {
               pruningStats.numEarlyPruning.fetch_add(1);
               state.personChecked[subgraphPersonId]=true;
               continue;
            }
         }
         state.personChecked[subgraphPersonId] = true;
         batchData.push_back(move(personData));
      }
      const uint32_t last = p-1;

      bool boundUpdated=false;
      if(batchData.size()>0) {
         //Run BFS
         BFSRunner::runBatch(batchData, state.subgraph);

         for(auto bIter=batchData.begin(); bIter!=batchData.end(); bIter++) {
            PersonEstimates& estimate = state.estimates.personEstimates[bIter->person];
            estimate.validate(bIter->componentSize, "after BFS");

            const auto closeness = getCloseness(state.numPersonsInForums, bIter->totalDistances, bIter->totalReachable);
            const PersonId externalPersonId = state.subgraph.mapFromSubgraph(bIter->person);
            CentralityResult resultCentrality(externalPersonId, bIter->totalDistances, bIter->totalReachable, closeness);
            pruningStats.numReachedPerson.fetch_add(bIter->totalReachable); 

            // Check if person qualifies as new top k value
            if(unlikely(!bIter->earlyExit)) {
               if(CentralityCmp::compare(make_pair(resultCentrality.person, resultCentrality), make_pair(centralityBound.person, centralityBound))) {
                  // Add improved value to top k list
                  lock_guard<mutex> lock(state.topResultsMutex);
                  state.topResults.insert(resultCentrality.person, resultCentrality);
                  state.globalCentralityBound.store(makeHeapBound(state.topResults.getBound().second));
                  CentralityResult updatedBound=*state.globalCentralityBound.load();
                  // Check if person truely updated the global bound
                  if(!(updatedBound==centralityBound)) {
                     boundUpdated=true;
                     state.lastBoundUpdate=last;
                     LOG_PRINT("[BoundUpdate] Order position (batch): "<<state.lastBoundUpdate<<" of "<<state.numPersonsInForums);
                  }
               }
            } else {
               pruningStats.numEarlyBfsExists.fetch_add(1);
            } 
         }
      }

      return make_pair(last-begin+1, boundUpdated);
   }

   void operator()() {
      #ifdef DEBUG
      auto startTime=awfy::chrono::now();
      #endif
      if(rangeEnd<rangeStart) {
         LOG_PRINT("[MorselTask] Fail! Invalid task range: "<<rangeStart<<"-"<<rangeEnd);
      }
      assert(rangeStart<=rangeEnd);

      uint32_t numConsecutivePrunedBFSs=0;
      uint32_t boundsStableThreshold=static_cast<uint32_t>((rangeEnd-rangeStart)*boundsStablePercentage);
      if(boundsStableThreshold<(minBoundRounds*6)) {
         boundsStableThreshold=minBoundRounds*6;
      }

      uint32_t rangeOffset=rangeStart;
      while(rangeOffset<rangeEnd) {
         bool boundUpdated;
         if(likely((abortOnceStable && rangeOffset<300) || rangeEnd-rangeOffset < 30)) {
            boundUpdated = processSinglePerson(rangeOffset, state.estimates.orderedPersons[rangeOffset]);
            rangeOffset++;
         } else {
            const auto batchResult = processPersonBatch(state.estimates.orderedPersons, rangeOffset, rangeEnd);
            rangeOffset += batchResult.first;
            boundUpdated = batchResult.second;
         }
         #ifdef DEBUG
         if(boundUpdated) {
            if(abortOnceStable) {
               pruningStats.numBoundImprovements.fetch_add(1);
            } else {
               pruningStats.numBoundImprovementsAfterInit.fetch_add(1);
            }
         }
         #endif

         // Check if the bound has stabilized enough to start parallel execution
         if(likely(abortOnceStable)) {
            numConsecutivePrunedBFSs=rangeOffset-state.lastBoundUpdate;
            if(unlikely(numConsecutivePrunedBFSs >= boundsStableThreshold)) {
               LOG_PRINT("[Query4 Seq] Finished sequential (started="<<startTime<<")");
               break;
            }
         }
      }

      _lastOffset = rangeOffset-1;
   }

   uint32_t lastProcessedOffset() const {
      return _lastOffset;
   }
};

struct ResultConcatenator {
   ScheduleGraph& taskGraph;
   QueryState* state;
   const char*& resultOut;
   PruningStats& pruningStats;
   ResultConcatenator(ScheduleGraph& taskGraph, QueryState* state, const char*& resultOut, PruningStats& pruningStats)
      : taskGraph(taskGraph), state(state), resultOut(resultOut), pruningStats(pruningStats)
   { }
   void operator()() {
      ostringstream output;
      auto& topEntries=state->topResults.getEntries();
      assert(topEntries.size()<=state->k);
      const uint32_t resNum = min(state->k, (uint32_t)topEntries.size());
      for (uint32_t i=0; i<resNum; i++){
         if(i>0) {
            output<<" ";
         }
         output<<topEntries[i].first;
      }
      LOG_PRINT("[Query4] Early pruning before BFS "<< pruningStats.numEarlyPruning.load());
      LOG_PRINT("[Query4] Early exit inside BFS "<< pruningStats.numEarlyBfsExists.load());
      LOG_PRINT("[Query4] Completed BFS "<< (state->numPersonsInForums-pruningStats.numEarlyPruning.load()-pruningStats.numEarlyBfsExists.load()));
      LOG_PRINT("[Query4] Bound improvements (during init) "<< pruningStats.numBoundImprovements.load());
      LOG_PRINT("[Query4] Bound improvements (after init) "<< pruningStats.numBoundImprovementsAfterInit.load());
      LOG_PRINT("[Query4] Reached "<< pruningStats.numReachedPerson.load()<<" of "<<((uint64_t)(state->numPersonsInForums-pruningStats.numReachedPerson.load()))*(uint64_t)(state->numPersonsInForums-1));
      LOG_PRINT("[Query4] neighbourPruning BFS "<<pruningStats.numNeighbourPruningBfs);
      #ifdef DEBUG
      #ifndef EXPBACKOFF
      for(PersonId id=1; id<state->subgraph.size(); id++) {
         if(!state->personChecked[id]) {
            LOG_PRINT("FAILED "<<id);
            for(PersonId j=0; j<state->estimates.orderedPersons.size(); j++) {
               if(state->estimates.orderedPersons[j]==id) {
                  LOG_PRINT("XXXXXX "<<j);
               }
            }
         }
         assert(state->personChecked[id]);
      }
      #endif
      #endif
      const auto& outStr = output.str();
      auto resultBuffer = awfy::Allocator::get().alloc<char>(outStr.size()+1);
      outStr.copy(resultBuffer, outStr.size());
      resultBuffer[outStr.size()]=0;
      resultOut = resultBuffer;
      taskGraph.updateTask(TaskGraph::Query4, -1);
      delete &pruningStats;
      delete state;
   }
};

pair<vector<char>,pair<uint32_t,uint64_t>> QueryRunner::buildPersonFilter(InterestId tagId) {
   uint32_t numPersons = personMapper.count();
   vector<char> personFilter(numPersons);

   // Get persons in forum and count degrees
   const auto forumLists = tagInForumsIndex.retrieve(tagId);
   if(likely(forumLists != tagInForumsIndex.end())) {
      auto forums = forumLists->firstList();
      do {
         auto forumBounds = forums->bounds();
         while(forumBounds.first != forumBounds.second) {
            const auto forumPersonLists = hasMemberIndex.retrieve(*forumBounds.first);
            if(unlikely(forumPersonLists==hasMemberIndex.end())) {
               forumBounds.first++;
               continue;
            }
            auto forumPersons = forumPersonLists->firstList();
            do {
               auto forumPersonsBounds=forumPersons->bounds();
               while(forumPersonsBounds.first != forumPersonsBounds.second) {
                  if(unlikely(!personFilter[*forumPersonsBounds.first])) {
                     personFilter[*forumPersonsBounds.first] = true;
                  }
                  forumPersonsBounds.first++;
               }
            } while((forumPersons=forumPersonLists->nextList(forumPersons)) != nullptr);
            forumBounds.first++;
         }
      } while((forums=forumLists->nextList(forums)) != nullptr);
   }

   uint32_t numPersonsInForums = 0;
   uint64_t numFriendsInForums = 0;
   // Filter out persons with no friends at all and no friends which are forum members
   for (PersonId person = 0; person<numPersons; ++person) {
      if(!personFilter[person]) { continue; }
      const auto friends = knowsIndex.retrieve(person);
      if(unlikely(friends==nullptr)) {
         personFilter[person]=false;
         continue;
      }
      // Check whether person has friend in forum
      bool hasFriend=false;
      auto friendsBounds = friends->bounds();
      while(friendsBounds.first != friendsBounds.second) {
         if(personFilter[*friendsBounds.first]) {
            hasFriend=true;
            numFriendsInForums++;
         }
         friendsBounds.first++;
      }
      if(likely(hasFriend)) {
         numPersonsInForums++;
      } else {
         personFilter[person]=false;
      }
   }

   return make_pair(move(personFilter), make_pair(numPersonsInForums, numFriendsInForums));
}

std::vector<PersonId> generateInterestingPersons(QueryState& state, ConnectedComponentStats* componentStats, uint32_t numPersonsInForums, const uint32_t numPersons) {
   std::mt19937 eng(numPersonsInForums); // This is the Mersenne Twister
   std::uniform_int_distribution<int> dist(1,numPersonsInForums-1);

   const uint32_t numPairs=numPersons*2;
   uint32_t discoveredPair=0;
   uint32_t attempts=0;
   std::vector<pair<PersonId,PersonId>> pairs;
   while(discoveredPair<numPairs && attempts<numPairs*3) {
      attempts++;

      // Sample two points
      PersonId start=dist(eng);
      PersonId end=dist(eng);
      assert(start<numPersonsInForums);
      assert(end<numPersonsInForums);

      // Check if same
      if(unlikely(start==end)) {
         continue;
      }

      // Check if in same connected component
      if(unlikely(componentStats->personComponents[start-1]!=componentStats->personComponents[end-1])) {
         continue;
      }

      pairs.push_back(make_pair(start, end));
      discoveredPair++;
   }

   // Collect path nodes
   BidirectSearchState searchState;
   std::vector<PersonId> interestingPersons;
   for(uint32_t i=0; i<pairs.size(); i++) {
      auto path=shortestPath(searchState, state.subgraph, pairs[i].first, pairs[i].second);
      for(uint32_t j=0; j<path.size(); j++) {
         interestingPersons.push_back(path[j]);
      }
   }

   sort(interestingPersons.begin(), interestingPersons.end());

   std::vector<pair<uint32_t, PersonId>> countingPersons;
   PersonId lastPerson=std::numeric_limits<PersonId>::max();
   for(uint32_t i=0; i<interestingPersons.size(); i++) {
      if(likely(interestingPersons[i]!=lastPerson)) {
         countingPersons.push_back(make_pair(1, interestingPersons[i]));
      } else {
         countingPersons[countingPersons.size()-1].first++;
      }
      lastPerson=interestingPersons[i];
   }

   sort(countingPersons.begin(), countingPersons.end());

   std::vector<PersonId> finalPersons;
   for(int32_t i=countingPersons.size()-1; i>=0; i--) {
      bool seenPerson=state.personChecked[countingPersons[i].second];
      if(unlikely(seenPerson)) {
         continue;
      }
      finalPersons.push_back(countingPersons[i].second);
      if(unlikely(finalPersons.size()==numPersons)) {
         break;
      }
   }

   return finalPersons;
}

struct SeenInterestingEstimateComparer {
   QueryState& state;

   SeenInterestingEstimateComparer(QueryState& state, const std::vector<PersonId> interestingPersons)
      : state(state) {
      for(uint32_t i=0; i<interestingPersons.size(); i++) {
         state.estimates.personEstimates[interestingPersons[i]].interesting=true;
      }
   }

   bool operator()(const PersonId& a, const PersonId& b) {
      const bool seenA = state.personChecked[a];
      const bool seenB = state.personChecked[b];

      if(seenA != seenB) {
         return seenA;
      }

      // Check interesting
      const bool interestingA = state.estimates.personEstimates[a].interesting;
      const bool interestingB = state.estimates.personEstimates[b].interesting;
      if(interestingA != interestingB) {
         return interestingA;
      }

      const auto distA=state.estimates.personEstimates[a].distances>>4;
      const auto distB=state.estimates.personEstimates[b].distances>>4;
      return distA < distB || (distA == distB && a < b );
   }
};

uint32_t getPersonsPerTask(uint32_t numRemaining) {
      uint32_t personsPerTask=morselSize;
      if(numRemaining/personsPerTask>maxMorselTasks) {
         personsPerTask=numRemaining/maxMorselTasks;
      }
      return personsPerTask;
}

struct SearchSpaceChunker {
   const uint32_t numPersonsInForums;
   QueryState& state;
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;

   const char*& resultOut;
   PruningStats* pruningStats;
   ConnectedComponentStats* componentStats;

   uint32_t lastChangePos;
   uint32_t lastOffset;
   uint32_t searchRound;

   SearchSpaceChunker(ScheduleGraph& taskGraph, Scheduler& scheduler, QueryState& state, const char*& resultOut, PruningStats* pruningStats, ConnectedComponentStats* componentStats, uint32_t lastChangePos, uint32_t lastOffset, uint32_t numPersonsInForums, uint32_t searchRound) 
      : numPersonsInForums(numPersonsInForums), state(state), taskGraph(taskGraph), scheduler(scheduler), resultOut(resultOut), pruningStats(pruningStats), componentStats(componentStats), lastChangePos(lastChangePos), lastOffset(lastOffset),  searchRound(searchRound) {
   }

   void operator()() {
      if(lastOffset==numPersonsInForums) {
         ResultConcatenator(taskGraph, &state, resultOut, *pruningStats)();
         return;
      }

      assert(state.lastBoundUpdate>0);
      // Check if search terminated
      if(searchRound%2==0) {
         if(searchRound>0 && state.lastBoundUpdate==lastChangePos) {
            LOG_PRINT("[SearchSpace] Finished after "<<lastOffset<<", last update "<<lastChangePos<<", ran for "<<searchRound<<" rounds");
            ResultConcatenator(taskGraph, &state, resultOut, *pruningStats)();
            return;
         } else {
            if(searchRound>0) {
               LOG_PRINT("[SearchSpace] Update in round "<<searchRound<<" updating from "<<lastChangePos<<" to "<<state.lastBoundUpdate); 
            }
            lastChangePos=state.lastBoundUpdate;
         }
      }

      if(searchRound%2==0) {
         uint32_t searchSpaceFactor=4;
         if(searchRound>0) {
            searchSpaceFactor=2;
         }
         // Update estimates and sort again
         auto interestingPersons=generateInterestingPersons(state, componentStats, numPersonsInForums, lastChangePos*searchSpaceFactor);
         sort(state.estimates.orderedPersons.begin()+lastOffset, state.estimates.orderedPersons.end(), SeenInterestingEstimateComparer(state, interestingPersons));
         LOG_PRINT("[SP] Looking at interesting persons "<<interestingPersons.size());
         uint32_t searchStartOffset=lastOffset;
         uint32_t searchEndOffset=searchStartOffset+interestingPersons.size();
         if(searchEndOffset>numPersonsInForums) {
            searchEndOffset=numPersonsInForums;
         }
         uint32_t searchSpaceSize=searchEndOffset-searchStartOffset;
         uint32_t personsPerTask=getPersonsPerTask(searchSpaceSize);
         uint32_t numTasks = (searchSpaceSize/personsPerTask);
         if(numTasks==0) {
            numTasks=1;
         }

         LOG_PRINT("[SearchSpace] Checking interesting from "<<searchStartOffset<<" to "<<searchEndOffset<<" in "<<numTasks<<" in round "<<searchRound<<". Last change: "<<lastChangePos);
         TaskGroup taskGroup;
         for(uint32_t task=0; task<numTasks; task++) {
            const PersonId rangeStart=searchStartOffset+personsPerTask*task;
            const PersonId rangeEnd=(uint32_t)task!=numTasks-1?(rangeStart+personsPerTask):searchEndOffset;
            LOG_PRINT("[SearchSpace] Scheduling task for range "<<rangeStart<<"-"<<rangeEnd);
            taskGroup.schedule(LambdaRunner::createLambdaTask(MorselTask(state, rangeStart, rangeEnd, *pruningStats, false, *componentStats),TaskGraph::Query4));
         }

         taskGroup.join(LambdaRunner::createLambdaTask(SearchSpaceChunker(taskGraph, scheduler, state, resultOut, pruningStats, componentStats, lastChangePos, searchEndOffset, numPersonsInForums, searchRound+1),TaskGraph::Query4));
         scheduler.schedule(taskGroup.close());
      } else {
         // Update estimates
         for(uint32_t i=lastOffset; i<state.estimates.orderedPersons.size(); i++) {
            const PersonId id=state.estimates.orderedPersons[i];
            const auto personReachable=componentStats->componentSizes[componentStats->personComponents[id-1]];
            updatePersonEstimate(state, id, personReachable);
         }
         // Sort estimates again
         sort(state.estimates.orderedPersons.begin()+lastOffset, state.estimates.orderedPersons.end(), EstimateComparer(state.estimates.personEstimates));

         // Calculate window size
         assert(lastChangePos<=lastOffset);
         uint32_t windowFactor = 28/((searchRound/2)+1);
         if(numPersonsInForums>800000) {
            windowFactor=windowFactor*2;
         }
         LOG_PRINT("[SearchSpace] Using factor "<<windowFactor<<" in round searchRound");
         const uint32_t verifyWindow = lastChangePos>minBoundRounds?lastChangePos*windowFactor:minBoundRounds*windowFactor;
         const uint32_t windowStart = lastOffset;
         const uint32_t windowEnd = min(lastOffset + verifyWindow, numPersonsInForums);
         assert(windowEnd<=numPersonsInForums);
         const uint32_t windowSize = windowEnd - windowStart;
         uint32_t personsPerTask=getPersonsPerTask(windowSize);
         uint32_t numTasks = (windowSize/personsPerTask);
         if(numTasks==0) {
            numTasks=1;
         }

         LOG_PRINT("[SearchSpace] Checking from "<<windowStart<<" to "<<windowEnd<<" in "<<numTasks<<" in round "<<searchRound<<". Last change: "<<lastChangePos);
         TaskGroup taskGroup;
         for(uint32_t task=0; task<numTasks; task++) {
            const PersonId rangeStart=windowStart+personsPerTask*task;
            const PersonId rangeEnd=(uint32_t)task!=numTasks-1?(rangeStart+personsPerTask):windowEnd;
            LOG_PRINT("[SearchSpace] Scheduling task for range "<<rangeStart<<"-"<<rangeEnd);
            taskGroup.schedule(LambdaRunner::createLambdaTask(MorselTask(state, rangeStart, rangeEnd, *pruningStats, false, *componentStats),TaskGraph::Query4));
         }

         taskGroup.join(LambdaRunner::createLambdaTask(SearchSpaceChunker(taskGraph, scheduler, state, resultOut, pruningStats, componentStats, lastChangePos, windowEnd, numPersonsInForums, searchRound+1),TaskGraph::Query4));
         scheduler.schedule(taskGroup.close());
      }
   }
};

TaskGroup QueryRunner::query(const uint32_t k, const char* tag, const char*& resultOut) {
   reset();

   //Get interest tag
   InterestId tagId = tagIndex.strToId.retrieve(awfy::StringRef(tag,strlen(tag)));
   if(tagId == tagIndex.strToId.end()) {
      //Tag not found
      auto resultBuffer = awfy::Allocator::get().alloc<char>(1);
      resultBuffer[0]=0;
      resultOut = resultBuffer;
      return TaskGroup();
   }

   //Build person filter
   const auto personFilterInfos = buildPersonFilter(tagId);
   const uint32_t numPersonsInForums = personFilterInfos.second.first;
   const uint64_t numFriendsInForums = personFilterInfos.second.second;

   //Build query subgraph
   PersonSubgraph subgraph(personFilterInfos.first, numPersonsInForums, numFriendsInForums, knowsIndex);
   auto componentStats = calculateConnectedComponents(subgraph);

   // Caculate estimates
   const PersonEstimatesData personEstimatesData = PersonEstimatesData::create(subgraph, *componentStats);

   QueryState* queryState = new QueryState(*this, k, numPersonsInForums, move(personEstimatesData), move(subgraph), getInitialBound());
   queryState->topResults.init(k);
   PruningStats* pruningStats = new PruningStats();

   //Process first persons to initialize top results
   PersonId numSequential=0;
   do {
      MorselTask sequentialInitialization(*queryState, numSequential, numPersonsInForums, *pruningStats, true, *componentStats);
      sequentialInitialization();
      numSequential = sequentialInitialization.lastProcessedOffset()+1;
      LOG_PRINT("[Query4] Sequential Loop "<<numSequential<<", last bound update:"<<queryState->lastBoundUpdate);
   } while(queryState->lastBoundUpdate==0 && numSequential<numPersonsInForums);
   LOG_PRINT("[Query4] Processed "<<numSequential<<" persons of "<<numPersonsInForums<<" sequentially");
   // Reoder after sequential part?

   //Schedule processing tasks
   TaskGroup taskGroup;
   taskGraph.updateTask(TaskGraph::Query4, 1);

   // Update estimates and sort again
   if(numSequential < numPersonsInForums) {
      for(uint32_t i=numSequential; i<queryState->estimates.orderedPersons.size(); i++) {
         const PersonId id=queryState->estimates.orderedPersons[i];
         const auto personReachable=componentStats->componentSizes[componentStats->personComponents[id-1]];
         updatePersonEstimate(*queryState, id, personReachable);
      }
      // Sort estimates again
      sort(queryState->estimates.orderedPersons.begin()+numSequential, queryState->estimates.orderedPersons.end(), EstimateComparer(queryState->estimates.personEstimates));
   }

   #ifndef EXPBACKOFF
   if(numSequential < numPersonsInForums) {
      const uint32_t numRemaining = numPersonsInForums-numSequential;
      // Limit number of tasks
      uint32_t personsPerTask=getPersonsPerTask(numRemaining);
      // Create tasks
      const uint32_t numTasks = numRemaining>personsPerTask?(numRemaining/personsPerTask):1;
      for(uint32_t task=0; task<numTasks; task++) {
         const PersonId rangeStart=numSequential+personsPerTask*task;
         const PersonId rangeEnd=(uint32_t)task!=numTasks-1?(rangeStart+personsPerTask):(numPersonsInForums);
         taskGroup.schedule(LambdaRunner::createLambdaTask(MorselTask(*queryState, rangeStart, rangeEnd, *pruningStats, false, *componentStats),TaskGraph::Query4));
      }
   }
   #else
   SearchSpaceChunker chunker(taskGraph, scheduler,*queryState, resultOut, pruningStats, componentStats, queryState->lastBoundUpdate, numSequential, numPersonsInForums, 0);
   chunker();
   #endif

   #ifndef EXPBACKOFF
   taskGroup.join(LambdaRunner::createLambdaTask(ResultConcatenator(taskGraph, queryState, resultOut, *pruningStats),TaskGraph::Query4));
   #endif

   return taskGroup;
}

}

