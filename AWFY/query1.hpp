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

#ifndef __QUERY1_H__
#define __QUERY1_H__

#include <array>
#include <deque>
#include "include/indexes.hpp"
#include "include/campers/hashtable.hpp"
#include "include/queue.hpp"

namespace Query1 {

   struct SearchState {
      campers::HashMap<PersonId,uint32_t> seen;
      awfy::Queue<pair<PersonId,uint32_t>> fringe;
      PersonId target;

      SearchState();
      void init(PersonId source, PersonId target);
   };

   struct BidirectSearchState {
      array<SearchState,2> states;
   };

   class QueryRunner {
      const PersonGraph& personGraph;
      const void* commentedGraph;
      BidirectSearchState searchState;

   public:
      QueryRunner(const FileIndexes& indexes);
      int query(PersonId p1, PersonId p2, int32_t num);
   };

}

#endif
