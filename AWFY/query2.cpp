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

#include "query2.hpp"

typedef std::pair<awfy::StringRef, uint32_t> InterestEntry;
namespace awfy {
template<>
class TopKComparer<InterestEntry> {
public:
   // Returns true if first param is larger or equal
   static bool compare(const InterestEntry& a, const InterestEntry& b) {
      return (a.second>b.second || (a.second==b.second && a.first<b.first));
   }
};
}

namespace Query2 {
   typedef awfy::TopKComparer<InterestEntry> Q2Comp;

   QueryRunner::QueryRunner(const FileIndexes& indexes) : 
      knowsIndex(*(indexes.personGraph)), birthdayIndex(indexes.birthdayIndex),
      hasInterestIndex(*(indexes.hasInterestIndex)), tagIndex(*(indexes.tagIndex)),
      personMapper(indexes.personMapper),
      interestStats(*indexes.interestStatistics),
      toVisit(personMapper.count())
   {
      {
         auto ret=posix_memalign(reinterpret_cast<void**>(&visited),64,personMapper.count()*sizeof(bool));
         if(unlikely(ret!=0)) {
            throw -1;
         }
      }
      {
         auto ret=posix_memalign(reinterpret_cast<void**>(&correctBirthday),64,personMapper.count()*sizeof(bool));
         if(unlikely(ret!=0)) {
            throw -1;
         }
      }
   }

   QueryRunner::~QueryRunner() {
   }

   void QueryRunner::reset() {
   }

   string QueryRunner::query(uint32_t num, uint32_t year, uint16_t month, uint16_t day) {
      reset();
      return connectedComponents_Simple(num, encodeBirthday(year,month,day));
   }

   // Checks whether birthday and interest match
   bool inline ignorePerson(const PersonId person, const InterestId interest, const HasInterestIndex& hasInterest) {
      // Skip persons that don't have the interest
      const auto personInterests=hasInterest.retrieve(person);
      if(likely(personInterests==nullptr || personInterests->find(interest)==nullptr)) {
         return true;
      } else {
         return false;
      }
   }

   uint32_t __attribute__((hot)) __attribute__((optimize("align-loops"))) getConnectedComponent(const PersonId person, const PersonGraph& knowsIndex, bool* __restrict__ visited, BFSQueue& toVisit, 
         const uint32_t numPersons, const uint32_t remainingPersons) {
      // This person is now a starting point for a connected component
      toVisit.reset(numPersons);
      {
         auto& p = toVisit.push_back_pos();
         p=person;
         visited[person]=true;
      }
      uint32_t componentSize=1;
      do {
         const PersonId curPerson = toVisit.front();
         toVisit.pop_front();

         const auto curFriends=knowsIndex.retrieve(curPerson);
         if(unlikely(curFriends==nullptr)) { continue; }

         auto friendsBounds = curFriends->bounds();
         while(friendsBounds.first != friendsBounds.second) {
            const PersonId curFriend=*friendsBounds.first;
            ++friendsBounds.first;
            if(likely(visited[curFriend])) { continue; }

            visited[curFriend]=true;
            componentSize++;
            auto& p = toVisit.push_back_pos();
            p = curFriend;
         }
      } while(!toVisit.empty() && (remainingPersons-componentSize)>0);

      return componentSize;
   }

   string __attribute__((hot)) __attribute__((optimize("align-loops"))) QueryRunner::connectedComponents_Simple(uint32_t k, const Birthday birthday) {
      const uint32_t numPersons=personMapper.count();
      memset(correctBirthday,0,numPersons);
      for(PersonId person=0;person<numPersons;person++) {
         if(birthdayIndex[person]>=birthday) {
            correctBirthday[person]=true;
         }
      }

      // Initialize top k list
      std::string worst("ZZZZZZZZZZZZZ");
      awfy::TopKList<awfy::StringRef,uint32_t> topResults(make_pair(awfy::StringRef(worst.c_str(), worst.size()), 0));
      topResults.init(k);

      // Iterate over all interests
      for(unsigned i=0; i<interestStats.size(); i++) {
         InterestStat interest=interestStats[i];
         // Skip interests that have less interests than the current bound
         if(likely(interest.numPersons<topResults.getBound().second)) { continue; }
         // Skip interest where birthdays would not match
         if(interest.maxBirthday<birthday) { continue; }
         // Get tag name
         const awfy::StringRef& tag = tagIndex.idToStr.retrieve(interest.interest);
         assert(tag.str!=nullptr);
         // Prune by name
         if(interest.numPersons==topResults.getBound().second && !Q2Comp::compare(InterestEntry(tag, interest.numPersons), topResults.getBound())) { continue; }
         // Skip interests which have no persons
         if(unlikely(interest.numPersons==0)) { continue; }

         // Reset seen list
         memset(visited,0,numPersons);

         // Ignore all people with bad birthdays or bad interests
         uint32_t matchingPersons=0;
         for(PersonId person=0; person<numPersons; person++) {
            if(unlikely(correctBirthday[person] && !ignorePerson(person, interest.interest, hasInterestIndex))) {
               matchingPersons++;
            } else {
               visited[person]=true;
            }
         }

         uint32_t maxComponentSize=0;
         uint32_t remainingPersons=matchingPersons;
         // Search next person which has this interest and is unprocessed
         for(PersonId person=0; person<numPersons; person++) {
            // Skip filtered persons (or were seen in a connected component)
            if(likely(visited[person])) { continue; }

            // Less persons remaining than needed for making the bound
            if(remainingPersons<topResults.getBound().second) { break; }

            uint32_t componentSize=getConnectedComponent(person, knowsIndex, visited, toVisit, numPersons, remainingPersons);
            remainingPersons-=componentSize;

            if(componentSize>maxComponentSize) {
               maxComponentSize=componentSize;
            }
         }

         // Insert into top k
         if(maxComponentSize>0) {
            topResults.insert(tag, maxComponentSize);
         }
      }

      // Create result string
      ostringstream output;
      auto& topEntries=topResults.getEntries();
      assert(topEntries.size()<=k);
      const uint32_t resNum = min(k, (uint32_t)topEntries.size());
      for (uint32_t i=0; i<resNum; i++) {
         if(i>0) {
            output<<" ";
         }
         output<<string(topEntries[i].first.str, topEntries[i].first.strLen);
      }

      return output.str();
   }

}
