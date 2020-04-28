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

#include <stack>
#include <functional>
#include <sstream>
#include "query3.hpp"
#include "include/indexers.hpp"

using namespace std;

bool compareLexicographic(const Query3::PersonPair& p1, const Query3::PersonPair& p2)
{
   return p1.first < p2.first || (p1.first == p2.first && p1.second < p2.second);
}

namespace awfy {
template<>
class TopKComparer<pair<Query3::PersonPair, uint32_t>> {
public:
   // Returns true if first param is larger or equal
   static bool compare(const pair<Query3::PersonPair, uint32_t>& a, const pair<Query3::PersonPair, uint32_t>& b)
   {
      int32_t delta = a.second - b.second;
      return (delta > 0) || ((delta == 0) && compareLexicographic(a.first, b.first));
   }
};
}

namespace Query3 {

QueryRunner::QueryRunner(const FileIndexes& fileIndexes)
   : knowsIndex(*(fileIndexes.personGraph)),
     personMapper(fileIndexes.personMapper),
     hasInterestIndex(*(fileIndexes.hasInterestIndex)),
     placeBoundsIndex(*(fileIndexes.placeBoundsIndex)),
     personPlaceIndex(*(fileIndexes.personPlaceIndex)),
     namePlaceIndex(*(fileIndexes.namePlaceIndex)),
     toVisit(personMapper.count()/2), // sufficient for test_1k
     topMatches(make_pair(PersonPair(numeric_limits<PersonId>::max(),numeric_limits<PersonId>::max()), 0)),
     seen(nullptr)
{
   bfsResults.reserve(512); // maximum number for 1k is 116
   personFilter.resize(personMapper.count());
   auto ret = posix_memalign(reinterpret_cast<void**>(&seen), 64, personMapper.count() * sizeof(bool));
   if(unlikely(ret!=0)) {
      throw -1;
   }
}

void QueryRunner::reset()
{
   placeBounds.clear();
   persons.clear();
   personFilter.clear();
}

bool mergeBounds(PlaceBounds& existingPlaceBounds, const PlaceBounds& curPlaceBounds)
{
   //Cases:
   //1: ||||     2: |||      3:    ||     4: |||||||  5:   ||||  6:      ||||    cur
   //     |||||         |||      |||||||       |||       ||||       |||          existing
   //
   bool mergedBound = false;
   if (curPlaceBounds.lower() <= existingPlaceBounds.lower()) {
      //Cases 1,2,4
      if (curPlaceBounds.upper() >= existingPlaceBounds.lower()) {
         //Cases 1,4
         existingPlaceBounds.lower() = curPlaceBounds.lower();
         mergedBound = true;
         if (curPlaceBounds.upper() > existingPlaceBounds.upper()) {
            //Case 4
            existingPlaceBounds.upper() = curPlaceBounds.upper();
         }
      }
   } else { //curPlaceBounds.lower>existingPlaceBounds.lower
      //Cases 3,5,6
      if (curPlaceBounds.upper() >= existingPlaceBounds.upper()) {
         //Cases 5,6
         if (curPlaceBounds.lower() <= existingPlaceBounds.upper()) {
            //Case 5
            mergedBound = true;
            existingPlaceBounds.upper() = curPlaceBounds.upper();
         }
      } else {
         //Case 3
         mergedBound = true;
      }
   }
   return mergedBound;
}

awfy::vector<PlaceBounds>&& QueryRunner::getPlaceBounds(const char* place)
{
   //Get place(s) bounds
   const awfy::StringRef placeName(place, strlen(place));
   auto placeIterRange = namePlaceIndex.equal_range(placeName);

   for (auto placeIter = placeIterRange.first; placeIter != placeIterRange.second; placeIter++) {
      const auto curPlaceBounds = placeBoundsIndex.at(placeIter->second);
      bool mergedBound = false;
      for (auto boundsIter = placeBounds.begin(); boundsIter != placeBounds.end(); boundsIter++) {
         mergedBound |= mergeBounds(*boundsIter, curPlaceBounds);
      }
      if (!mergedBound) {
         placeBounds.push_back(curPlaceBounds);
      }
   }
   //Merge overlapping bounds
   if (placeBounds.size() > 1) {
      restartMerge: for (size_t a = 0; a < placeBounds.size() - 1; a++) {
         for (size_t b = a + 1; b < placeBounds.size(); b++) {
            if (mergeBounds(placeBounds[a], placeBounds[b])) {
               placeBounds.erase(placeBounds.begin() + b);
               goto restartMerge;
            }
         }
      }
   }
   return move(placeBounds);
}

#ifdef DEBUG
/// Assert that interests are sorted. This is needed for mergon join
template<class T>
void checkInOrder(const T interests) {
   auto count = interests->size();
   InterestId lastId = 0;
   for (unsigned i = 0; i < count; ++i) {
      auto interestId=*interests->getPtr(i);
      assert(lastId==0||lastId<interestId);
      lastId=interestId;
   }
}
#endif

void __attribute__((hot)) __attribute__((optimize("align-loops"))) QueryRunner::runBFS(PersonId start, uint32_t hops) {
   assert(toVisit.empty()); //Data structures are in a sane state
   memset(seen, 0, personMapper.count() * sizeof(bool));

   seen[start] = true;
   bfsResults.clear();

   {
      auto& p = toVisit.push_back_pos();
      p.first=start;
      p.second=0;
   }
   do {
      const PersonId curPerson = toVisit.front().first;
      const uint32_t curDist = toVisit.front().second;
      toVisit.pop_front();

      if (unlikely(curDist+1>hops)) {
         toVisit.clear();
         return;
      }

      const auto curFriends = knowsIndex.retrieve(curPerson);
      if (unlikely(curFriends==nullptr)) {
         continue;
      }

      auto friendsBounds = curFriends->bounds();
      while (friendsBounds.first != friendsBounds.second) {
         const PersonId curFriend = *friendsBounds.first;
         ++friendsBounds.first;
         if (seen[curFriend]) {
            continue;
         }
         if (curFriend > start && personFilter[curFriend]) {
            bfsResults.push_back(curFriend);
         }
         seen[curFriend] = true;
         auto& p = toVisit.push_back_pos();
         p.first=curFriend;
         p.second=curDist+1;
      }
   } while (!toVisit.empty());
}

#ifdef SSE_INTEREST_COUNT
/**
 * SSE_INTEREST_COUNT:
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * Original code by Daniel Lemire, Leonid Boytsov, Nathan Kurz:
 * http://arxiv.org/abs/1401.6399
 */
size_t match_scalar(const uint32_t *A, const size_t lenA,
                    const uint32_t *B, const size_t lenB) {
    uint64_t count=0;
    if (lenA == 0 || lenB == 0) return 0;

    const uint32_t *endA = A + lenA;
    const uint32_t *endB = B + lenB;

    while (1) {
        while (*A < *B) {
SKIP_FIRST_COMPARE:
            if (++A == endA) goto FINISH;
        }
        while (*A > *B) {
            if (++B == endB) goto FINISH;
        }
        if (*A == *B) {
            ++count;
            if (++A == endA || ++B == endB) goto FINISH;
        } else {
            goto SKIP_FIRST_COMPARE;
        }
    }

FINISH:
    return count;
}

#define VEC_T __m128i

#define VEC_OR(dest, other)                                             \
    __asm volatile("por %1, %0" : "+x" (dest) : "x" (other) )

#define VEC_ADD_PTEST(var, add, xmm)      {                             \
        decltype(var) _new = var + add;                                   \
        __asm volatile("ptest %2, %2\n\t"                           \
                       "cmovnz %1, %0\n\t"                          \
                       : "+r" (var)                    \
                       : "r" (_new), "x" (xmm)         \
                       : "cc");                      \
}

#define VEC_CMP_EQUAL(dest, other)                                      \
    __asm volatile("pcmpeqd %1, %0" : "+x" (dest) : "x" (other))

#define VEC_SET_ALL_TO_INT(reg, int32)                                  \
    __asm volatile("movd %1, %0; pshufd $0, %0, %0"                 \
                       : "=x" (reg) : "g" (int32) )

#define VEC_LOAD_OFFSET(xmm, ptr, bytes)                    \
    __asm volatile("movdqu %c2(%1), %0" : "=x" (xmm) :  \
                   "r" (ptr), "i" (bytes))

#define COMPILER_LIKELY(x)     __builtin_expect((x),1)
#define COMPILER_RARELY(x)     __builtin_expect((x),0)

#define ASM_LEA_ADD_BYTES(ptr, bytes)                            \
    __asm volatile("lea %c1(%0), %0\n\t" :                       \
                   "+r" (ptr) :           \
                   "i" (bytes));

size_t v1
(const uint32_t *rare, size_t lenRare,
 const uint32_t *freq, size_t lenFreq) {
    assert(lenRare <= lenFreq);

    uint64_t count=0;
    if (lenFreq == 0 || lenRare == 0) return 0;

    const uint64_t kFreqSpace = 2 * 4 * (0 + 1) - 1;
    const uint64_t kRareSpace = 0;

    const uint32_t *stopFreq = &freq[lenFreq] - kFreqSpace;
    const uint32_t *stopRare = &rare[lenRare] - kRareSpace;

    VEC_T Rare;

    VEC_T F0, F1;

    if (COMPILER_RARELY((rare >= stopRare) || (freq >= stopFreq))) goto FINISH_SCALAR;

    uint64_t valRare;
    valRare = rare[0];
    VEC_SET_ALL_TO_INT(Rare, valRare);

    uint64_t maxFreq;
    maxFreq = freq[2 * 4 - 1];
    VEC_LOAD_OFFSET(F0, freq, 0 * sizeof(VEC_T)) ;
    VEC_LOAD_OFFSET(F1, freq, 1 * sizeof(VEC_T));

    if (COMPILER_RARELY(maxFreq < valRare)) goto ADVANCE_FREQ;

ADVANCE_RARE:
    do {
        valRare = rare[1];
        ASM_LEA_ADD_BYTES(rare, sizeof(*rare));

        if (COMPILER_RARELY(rare >= stopRare)) {
            rare -= 1;
            goto FINISH_SCALAR;
        }

        VEC_CMP_EQUAL(F0, Rare) ;
        VEC_CMP_EQUAL(F1, Rare);

        VEC_SET_ALL_TO_INT(Rare, valRare);

        VEC_OR(F0, F1);

        VEC_ADD_PTEST(count, 1, F0);

        VEC_LOAD_OFFSET(F0, freq, 0 * sizeof(VEC_T)) ;
        VEC_LOAD_OFFSET(F1, freq, 1 * sizeof(VEC_T));

    } while (maxFreq >= valRare);

    uint64_t maxProbe;

ADVANCE_FREQ:
    do {
        const uint64_t kProbe = (0 + 1) * 2 * 4;
        const uint32_t *probeFreq = freq + kProbe;
        maxProbe = freq[(0 + 2) * 2 * 4 - 1];

        if (COMPILER_RARELY(probeFreq >= stopFreq)) {
            goto FINISH_SCALAR;
        }

        freq = probeFreq;

    } while (maxProbe < valRare);

    maxFreq = maxProbe;

    VEC_LOAD_OFFSET(F0, freq, 0 * sizeof(VEC_T)) ;
    VEC_LOAD_OFFSET(F1, freq, 1 * sizeof(VEC_T));

    goto ADVANCE_RARE;

FINISH_SCALAR:
    lenFreq = stopFreq + kFreqSpace - freq;
    lenRare = stopRare + kRareSpace - rare;

    size_t tail = match_scalar(freq, lenFreq, rare, lenRare);

    return count + tail;
}

template<class T>
uint32_t getCommonInterestCount(const T& list1, const T& list2)
{
   auto bounds1 = list1->bounds();
   auto bounds2 = list2->bounds();
   const uint32_t* i1 = bounds1.first, *i2 = bounds2.first;
   size_t size1 = bounds1.second - bounds1.first;
   size_t size2 = bounds2.second - bounds2.first;

   if (size1 <= size2)
      return v1(i1, size1, i2, size2); else
      return v1(i2, size2, i1, size1);
}
#else //SSE_INTEREST_COUNT
template<class T>
uint32_t getCommonInterestCount(const T& list1, const T& list2){
   uint32_t res = 0;
   auto bounds1 = list1->bounds();
   auto bounds2 = list2->bounds();
   auto last2 = bounds2.second-1;
   while(bounds1.first != bounds1.second) {
      while(bounds2.first<last2 && *bounds2.first<*bounds1.first) { ++bounds2.first; }
      res+=(*bounds2.first==*bounds1.first);
      ++bounds1.first;
   }
   return res;
}
#endif

#ifdef Q3_SORT_BY_INTEREST
struct PersonNumInterestSorter {
   inline bool operator()(const pair<PersonId,uint32_t>& c1, const pair<PersonId,uint32_t>& c2) {
      return c1.second>c2.second || (c1.second==c2.second && c1.first<c2.first);
   }
};
#endif

void QueryRunner::buildPersonFilter(const awfy::vector<PlaceBounds>& place)
{
   const uint32_t numPersons=personMapper.count();
   personFilter.resize(numPersons);

   if (place.size() == 1) {
      for (PersonId person = 0; person < numPersons; person++) {
         if (unlikely(personAtPlace(person,place[0],personPlaceIndex))) {
            #ifdef Q3_SORT_BY_INTEREST
            persons.push_back(make_pair(person,hasInterestIndex.retrieve(person)->size()));
            #else
            persons.push_back(person);
            #endif
            personFilter[person] = true;
         }
      }
   } else {
      for (PersonId person = 0; person < numPersons; person++){
         bool atPlace = false;
         for(auto pIter=place.cbegin(); pIter!=place.cend(); pIter++) {
            if(personAtPlace(person, *pIter, personPlaceIndex)) {
               atPlace=true;
               break;
            }
         }
         if(atPlace) {
            #ifdef Q3_SORT_BY_INTEREST
            persons.push_back(make_pair(person,hasInterestIndex.retrieve(person)->size()));
            #else
            persons.push_back(person);
            #endif
            personFilter[person] = true;
         }
      }
   }
   #ifdef Q3_SORT_BY_INTEREST
   sort(persons.begin(), persons.end(), PersonNumInterestSorter());
   #endif
}

string QueryRunner::queryPlaces(const uint32_t k, uint32_t hops, const awfy::vector<PlaceBounds>& place) {
   topMatches.init(k);
   
   // collect all persons
   buildPersonFilter(place); //Maximum id of a person considered in the query

   for(auto personIter=persons.cbegin(); personIter!=persons.cend(); personIter++) {
      #ifdef Q3_SORT_BY_INTEREST
      const auto personId = personIter->first;
      const auto personInterestCount = personIter->second;
      // Skip persons that have too few interests to make the top k bound
      if(personInterestCount<topMatches.getBound().second
         || (personInterestCount==topMatches.getBound().second 
             && compareLexicographic(topMatches.getBound().first, PersonPair(personId,numeric_limits<PersonId>::max())))) {
         continue;
      }
      #else
      const auto personId = *personIter;
      const auto ownInterests = hasInterestIndex.retrieve(personId);
      if(ownInterests->size()<topMatches.getBound().second
         || (ownInterests->size()==topMatches.getBound().second 
             && compareLexicographic(topMatches.getBound().first, PersonPair(personId,numeric_limits<PersonId>::max())))) {
         continue;
      }
      #endif

      runBFS(personId, hops);

      #ifdef Q3_SORT_BY_INTEREST
      auto const ownInterests = hasInterestIndex.retrieve(personId);
      #endif

      // Iterate over all persons within "hops" distance && place bounds and calculate common interests
      uint64_t invertedPersonId=personMapper.invert(personId);
      for(auto friendIter=bfsResults.cbegin(); friendIter!=bfsResults.cend(); friendIter++) {
         const auto friendId = *friendIter;
         assert(friendId >= personId);

         const auto friendsInterests = hasInterestIndex.retrieve(friendId);
         // Skip reachable person if it has too few interests to make top k bound
         if(friendsInterests->size()<topMatches.getBound().second
            || (friendsInterests->size()==topMatches.getBound().second 
             && compareLexicographic(topMatches.getBound().first, PersonPair(personId,friendId)))) {
            continue;
         }

         // Calculate common interests and update top k list
         const auto commonInterests = getCommonInterestCount(ownInterests, friendsInterests);
         topMatches.insert(
            make_pair(invertedPersonId, personMapper.invert(friendId)),
            commonInterests);
      } 
   }

   ostringstream output;
   auto matches = topMatches.getEntries();
   const uint32_t resNum = min(k, (uint32_t)matches.size());
   for (uint32_t i=0; i<resNum; i++){
      if(i>0) {
         output<<" ";
      }
      const auto& resultPair = matches[i].first;
      output<<resultPair.first<<"|"<<resultPair.second;
   }

   return output.str();
}

string QueryRunner::query(const uint32_t k, const uint32_t hops, const char* place) {
   reset();
   
   awfy::vector<PlaceBounds> placeBounds = getPlaceBounds(place);
   if(unlikely(placeBounds.size()==0)) {
      //Handle case that an invalid place is queried
      ostringstream output;
      return output.str();
   }

   return queryPlaces(k,hops,placeBounds);
}

}
