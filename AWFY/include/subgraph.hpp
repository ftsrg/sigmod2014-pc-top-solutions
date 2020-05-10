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

#include "indexes.hpp"
#include "queue.hpp"

class PersonSubgraph {
#ifdef Q4_BUILD_SUBGRAPH
private:
   uint32_t numSubgraphPersons;

   vector<PersonId> mapTo;
   vector<PersonId> mapFrom;

   PersonGraph subgraph;

public:
   PersonSubgraph(const vector<char>& nodeFilter, PersonId numElements, uint64_t numSubgraphFriends, const PersonGraph& personGraph)
      : numSubgraphPersons(numElements+1), mapTo(nodeFilter.size()), mapFrom(numSubgraphPersons), subgraph(numSubgraphPersons)
   {
      //Map ids retaining order
      PersonId filterPos=0;
      PersonId nextId=1;
      for(auto filterIter=nodeFilter.cbegin(); filterIter!=nodeFilter.cend(); filterIter++, filterPos++) {
         if(*filterIter) {
            assert(nextId<numSubgraphPersons);
            mapTo[filterPos] = nextId;
            mapFrom[nextId] = filterPos;
            nextId++;
         }
      }

      size_t memorySize=numSubgraphPersons*sizeof(PersonId)+numSubgraphFriends*sizeof(PersonId);
      LOG_PRINT("[Subgraph] Subgraph size: "<<memorySize/1024<<" kb compared to PersonGraph size "<<personGraph.buffer.size/1024<<"kb");
      PersonId* subgraphData = new PersonId[memorySize/sizeof(PersonId)];
      #ifdef DEBUG
      PersonId* subgraphDataEnd = subgraphData + memorySize/sizeof(PersonId);
      #endif

      PersonId* subgraphDataPos = subgraphData;
      for(PersonId i=0; i<nodeFilter.size(); i++) {
         const PersonId subgraphId = mapTo[i];
         if(subgraphId>0) {
            const auto personFriends = personGraph.retrieve(i);
            assert(personFriends!=nullptr);

            const auto personDataStart = subgraphDataPos;
            assert(personDataStart < subgraphDataEnd);
            *personDataStart = 0;
            subgraphDataPos++;

            auto friendsBounds = personFriends->bounds();
            while(friendsBounds.first != friendsBounds.second) {
               const PersonId friendSubgraphId = mapTo[*friendsBounds.first];
               if(friendSubgraphId>0) {
                  (*personDataStart)++;
                  assert(subgraphDataPos < subgraphDataEnd);
                  *subgraphDataPos = friendSubgraphId;
                  subgraphDataPos++;
               }
               friendsBounds.first++;
            }
            subgraph.insert(subgraphId, reinterpret_cast<SizedList<uint32_t,PersonId>*>(personDataStart));
         }
      }
      subgraph.buffer.data = subgraphData;
      subgraph.buffer.size = (subgraphDataPos-subgraphData)*sizeof(PersonId);
   }

   PersonSubgraph(PersonSubgraph&& other)
      : numSubgraphPersons(other.numSubgraphPersons), mapTo(move(other.mapTo)), mapFrom(move(other.mapFrom)), subgraph(move(other.subgraph))
   { }

   ~PersonSubgraph() {
      if(subgraph.buffer.data != nullptr) {
         delete[] (PersonId*)subgraph.buffer.data;
      }
   }

   inline bool personInSubgraph(__attribute__((unused)) PersonId id) const __attribute__((always_inline)) {
      if(unlikely(id==0)) {
         return false;
      }
      return true;
   }

   #ifdef DEBUG
   inline void assertInSubgraph(PersonId id) const __attribute__((always_inline)) {
      assert(id!=0);
   }
   #else
   inline void assertInSubgraph(PersonId) const __attribute__((always_inline)) { }
   #endif

   inline const PersonGraph& graph() const {
      return subgraph;
   }

   inline uint32_t size() const {
      return numSubgraphPersons;
   }

   inline PersonId mapToSubgraph(PersonId id) const {
      assert(id<mapTo.size());
      return mapTo[id];
   }
   inline PersonId mapFromSubgraph(PersonId id) const {
      assert(id<mapFrom.size());
      return mapFrom[id];
   }
#else //Q4_BUILD_SUBGRAPH
private:
   uint32_t numPersons;
   const vector<char>& nodeFilter;
   const PersonGraph& personGraph;

public:
   PersonSubgraph(const vector<char>& nodeFilter, __attribute__((unused)) PersonId numElements, const PersonGraph& personGraph)
      : numPersons(nodeFilter.size()), nodeFilter(nodeFilter), personGraph(personGraph)
   { }

   inline bool personInSubgraph(PersonId id) const {
      return nodeFilter[id];
   }

   inline const PersonGraph& graph() const {
      return personGraph;
   }

   inline uint32_t size() const {
      return numPersons;
   }

   inline PersonId mapToSubgraph(PersonId id) const __attribute__((always_inline)) {
      return id;
   }
   inline PersonId mapFromSubgraph(PersonId id) const __attribute__((always_inline)) {
      return id;
   }
#endif
};