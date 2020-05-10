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

#include "query1.hpp"

namespace Query1 {

   SearchState::SearchState()
      : seen(1024), fringe(1024), target(0) {
   }

   // Reset data structures and initialize for search
   void SearchState::init(PersonId source, PersonId target) {
      seen.clear();
      fringe.clear();
      this->target=target;
      auto& p = fringe.push_back_pos();
      p.first=source;
      p.second=0;
   }

   QueryRunner::QueryRunner(const FileIndexes& indexes) 
      : personGraph(*(indexes.personGraph)), commentedGraph(indexes.personCommentedGraph) {
   }

   template<bool checkCommented>
   int shortestPath(BidirectSearchState& searchState, const PersonGraph& personGraph, const void* commentedGraph, PersonId p1, PersonId p2, uint32_t num) {
      // Prepare access to index data structures
      typedef typename std::remove_pointer<typename PersonGraph::Content>::type Content;
      auto basePersonPtr = reinterpret_cast<uint8_t*>(personGraph.buffer.data);
      auto baseCommentedPtr = reinterpret_cast<const uint8_t*>(commentedGraph);

      // Reset data structures and initialize for this search
      assert(searchState.states.size()==2);
      auto& bidiStates=searchState.states;
      bidiStates[0].init(p1, p2);
      bidiStates[1].init(p2, p1);

      // Run bidirectional search
      int8_t dir = 0;
      bool bidiJoined[2]={false,false};
      unsigned resultDist=std::numeric_limits<unsigned>::max();
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
         if (unlikely(bidiJoined[1-dir]&&otherDirSeen.count(curPerson))) {
            return resultDist;
         }

         // Load neighbors and comment information
         const auto neighbours = personGraph.retrieve(curPerson);
         if(unlikely(neighbours==nullptr)) {
            continue;
         }
         auto neighbourCount = neighbours->size();
         const auto neighboursOffset = reinterpret_cast<const uint8_t*>(neighbours)-basePersonPtr;
         const auto commentedCounts = reinterpret_cast<const Content*>(baseCommentedPtr+neighboursOffset);

         // Continue search over friends
         for (unsigned i = 0; i < neighbourCount; ++i) {
            auto neighbourId = *neighbours->getPtr(i);
            // Skip already seen neighbors
            if(dirSeen.count(neighbourId)) {
               continue;
            }

            // Skip persons that don't fulfill  the comment criteria
            if(checkCommented) {
               // Only process those that have commented enough and were not seen yet
               if(unlikely(*commentedCounts->getPtr(i)>num)) {
                  // Check if reverse is also true
                  auto otherNeighbours= personGraph.retrieve(neighbourId);
                  assert(otherNeighbours!=nullptr);
                  auto otherNeighbourOffset = otherNeighbours->find(curPerson);
                  assert(otherNeighbourOffset!=nullptr);
                  auto commentedOffset = reinterpret_cast<const uint8_t*>(otherNeighbourOffset)-basePersonPtr;
                  if(unlikely(*reinterpret_cast<const PersonGraph::Id*>(baseCommentedPtr+commentedOffset)<=num)) {
                     continue;
                  }
               } else {
                  continue;
               }
            }

            auto neighbourDist=curDepth+1;

            // Return if we found the target
            if(unlikely(neighbourId==dirTarget)) {
               return neighbourDist;
            }
            // Insert neighbor distance information
            dirSeen.tryInsert(neighbourId)[0]=neighbourDist;
            dirFringe.push_back(make_pair(neighbourId, neighbourDist));

            // Check whether the bidirectional searches meet
            auto otherSeenNeighbour=otherDirSeen.find(neighbourId);
            if (unlikely(otherSeenNeighbour!=nullptr)) {
               auto joinedDist=neighbourDist+*otherSeenNeighbour;
               if(unlikely(resultDist>joinedDist)) {
                  resultDist=joinedDist; bidiJoined[dir]=true;
               }
            }

         }
      }
      return -1;
   }

   int QueryRunner::query(PersonId p1, PersonId p2, int32_t num) {
      // Shortcut when source == target
      if(unlikely(p1==p2)) {
         return 0;
      }

      // Calculate shortest path from source to target
      if(unlikely(num>=0)) {
         return shortestPath<true>(searchState,personGraph,commentedGraph,p1,p2,num);
      } else {
         return shortestPath<false>(searchState,personGraph,commentedGraph,p1,p2,num);
      }
   }
}
