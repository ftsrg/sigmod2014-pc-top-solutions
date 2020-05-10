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
#include "include/topklist.hpp"
#include "include/indexes.hpp"
#include "include/alloc.hpp"
#include "include/queue.hpp"
#include "query4.hpp"

#define SSE_INTEREST_COUNT

using namespace std;

namespace Query3 {

typedef std::pair<PersonId, PersonId> PersonPair;

class QueryRunner {
   //Indexes
   const PersonGraph& knowsIndex;
   const PersonMapper& personMapper;
   const HasInterestIndex& hasInterestIndex;
   const PlaceBoundsIndex& placeBoundsIndex;
   const PersonPlaceIndex& personPlaceIndex;
   const NamePlaceIndex& namePlaceIndex;

   typedef awfy::TopKList<PersonPair, uint32_t> TopKPairs;

   //Runtime data
   awfy::vector<PlaceBounds> placeBounds;
   #ifdef Q3_SORT_BY_INTEREST
   awfy::vector<pair<PersonId,uint32_t>> persons;
   #else
   awfy::vector<PersonId> persons;
   #endif
   awfy::vector<char> personFilter;
   awfy::Queue<std::pair<PersonId, uint32_t>> toVisit;
   awfy::vector<PersonId> bfsResults;
   TopKPairs topMatches;
   bool* seen;

   void reset();

   void runBFS(PersonId start, uint32_t hops);
   awfy::vector<PlaceBounds>&& getPlaceBounds(const char* place);

   /// Builds person filter and returns max. person id in the filter
   void buildPersonFilter(const awfy::vector<PlaceBounds>& place);
   string queryPlaces(const uint32_t k, uint32_t hops, const awfy::vector<PlaceBounds>& place);

public:
   QueryRunner(const FileIndexes& indexes);
   string query(const uint32_t k, const uint32_t hops, const char* place);
};
}
