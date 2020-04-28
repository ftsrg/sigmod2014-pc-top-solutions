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

#ifndef __QUERY2_H__
#define __QUERY2_H__

#include <string>
#include <deque>
#include "include/indexes.hpp"
#include "include/alloc.hpp"
#include "include/campers/hashtable.hpp"
#include "include/MurmurHash2.h"
#include "include/topklist.hpp"
#include "include/queue.hpp"

using namespace std;

namespace Query2 {

   typedef awfy::FixedSizeQueue<PersonId> BFSQueue;

   class QueryRunner {
      const PersonGraph& knowsIndex;
      const Birthday* birthdayIndex;
      const HasInterestIndex& hasInterestIndex;
      const TagIndex& tagIndex;
      const PersonMapper& personMapper;
      const InterestStatistics& interestStats;

      // Query data structures
      bool* correctBirthday;
      bool* visited;
      BFSQueue toVisit;

      void reset();
      string connectedComponents_Simple(uint32_t num, const Birthday birthday);

   public:
      QueryRunner(const FileIndexes& indexes);
      ~QueryRunner();
      string query(uint32_t num, uint32_t year, uint16_t month, uint16_t day);
   };

}

#endif
