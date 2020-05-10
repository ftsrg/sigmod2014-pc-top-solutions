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

#include "io.hpp"
#include "util/log.hpp"
#include "boost/unordered_set.hpp"
#include "StringRef.hpp"
#include "hash.hpp"
#include "campers/hashtable.hpp"
#include "concurrent/atomic.hpp"

using boost::unordered_set;

namespace queryfiles {

   /// Parser for the query files
   class QueryParser {
   public:
      struct BaseQuery {
         char id;
      };

      struct Query1 : public BaseQuery {
         static const char QueryId = '1';
         PersonId p1;
         PersonId p2;
         int32_t x;
      };

      struct Query2 : public BaseQuery {
         static const char QueryId = '2';
         uint32_t k;
         uint16_t year;
         uint8_t month;
         uint8_t day;
      };

      struct Query3 : public BaseQuery {
         static const char QueryId = '3';
         uint32_t k;
         uint32_t hops;
         const char* getPlace() {
            return reinterpret_cast<const char*>(this)+sizeof(Query3);
         }
      };

      struct Query4 : public BaseQuery {
         static const char QueryId = '4';
         uint32_t k;
         const char* getTag() {
            return reinterpret_cast<const char*>(this)+sizeof(Query4);
         }
      };

      virtual ~QueryParser() = default;

      static inline size_t maxQuerySize() {
         return 1024;
      }

      static inline size_t getQueryIndex(char queryId) {
         return queryId - 49;
      }

      virtual int64_t readNext(void* resultPtr) = 0;
   };

   class QueryFileParser : public QueryParser {
      tokenize::Tokenizer tokenizer;

   public:

      QueryFileParser(io::MmapedFile& file);
      QueryFileParser(const QueryParser&) = delete;
      QueryFileParser(QueryParser&&) = delete;

      static inline size_t maxQuerySize() {
         return 1024;
      }

      static inline size_t getQueryIndex(char queryId) {
         return queryId - 49;
      }

      int64_t readNext(void* resultPtr) override {
         if(!tokenizer.finished()) {
            auto iter=tokenizer.getPositionPtr();
            if(iter[5] == Query1::QueryId) {
               return parseQ1(resultPtr);
            } else if(iter[5] == Query2::QueryId) {
               return parseQ2(resultPtr);
            } else if(iter[5] == Query3::QueryId) {
               return parseQ3(resultPtr);
            } else if(iter[5] == Query4::QueryId) {
               return parseQ4(resultPtr);
            } else {
               FATAL_ERROR("Invalid query id "<<(unsigned) iter[5]);
               return -1;
            }
         } else {
            return -1;
         }
      }

   private:
      inline size_t parseQ1(void* buffer) {
         tokenizer.skip(7);
         PersonId p1=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         PersonId p2=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         int32_t x=tokenizer.consumeLong(')');
         tokenizer.skip(1);

         Query1* result = reinterpret_cast<Query1*>(buffer);
         result->id = Query1::QueryId;
         result->p1 = p1;
         result->p2 = p2;
         result->x = x;

         return sizeof(Query1);
      }

      inline size_t parseQ2(void* buffer) {
         tokenizer.skip(7);
         uint32_t k=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         uint16_t year=tokenizer.consumeLongChars(4,1);
         uint16_t month=tokenizer.consumeLongChars(2,1);
         uint16_t day=tokenizer.consumeLongChars(2,2);

         Query2* result = reinterpret_cast<Query2*>(buffer);
         result->id = Query2::QueryId;
         result->k = k;
         result->year = year;
         result->month = month;
         result->day = day;

         return sizeof(Query2);
      }

      inline size_t parseQ3(void* buffer) {
         tokenizer.skip(7);
         uint32_t k=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         uint32_t hops=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         auto strPtr=tokenizer.getPositionPtr();
         auto strLen=tokenizer.skipAfterAndCount(')')-1;
         tokenizer.skip(1);

         // Copy string into query buffer
         auto strTarget = reinterpret_cast<char*>(buffer) + sizeof(Query3);
         for (unsigned i=0;i<strLen;++i) // for loop often superior to memcpy
            *(strTarget+i)=*(strPtr+i);
         strTarget[strLen] = 0;

         Query3* result = reinterpret_cast<Query3*>(buffer);
         result->id = Query3::QueryId;
         result->k = k;
         result->hops = hops;

         return sizeof(Query3)+strLen+1;
      }

      inline size_t parseQ4(void* buffer) {
         tokenizer.skip(7);
         uint32_t k=tokenizer.consumeLong(',');
         tokenizer.skip(1);
         auto strPtr=tokenizer.getPositionPtr();
         auto strLen=tokenizer.skipAfterAndCount(')')-1;
         tokenizer.skip(1);

         // Copy string into query buffer
         auto strTarget = reinterpret_cast<char*>(buffer) + sizeof(Query4);
         for (unsigned i=0;i<strLen;++i) // for loop often superior to memcpy
            *(strTarget+i)=*(strPtr+i);
         strTarget[strLen] = 0;

         Query4* result = reinterpret_cast<Query4*>(buffer);
         result->id = Query4::QueryId;
         result->k = k;

         return sizeof(Query4)+strLen+1;
      }
   };

   /// Answer file parser
   class AnswerParser {
      tokenize::Tokenizer tokenizer;

   public:
      AnswerParser(io::MmapedFile& file);
      AnswerParser(const AnswerParser&) = delete;
      AnswerParser(AnswerParser&&) = delete;

      inline string readAnswer() {
         auto strPtr=tokenizer.getPositionPtr();
         auto strLen=tokenizer.skipAfterAndCount('%')-2;
         tokenizer.skipAfter('\n');

         assert(strPtr!=nullptr);
         return string(strPtr,strLen);
      }

      inline void skipLine() {
         tokenizer.skipAfter('\n');
      }
   };

   QueryFileParser::QueryFileParser(io::MmapedFile& file) : tokenizer(file) {
      // -1 because assumes ending with newline
      tokenizer.limit=tokenizer.limit-1;
      madvise(file.mapping,file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   }

   AnswerParser::AnswerParser(io::MmapedFile& file) : tokenizer(file) {
      // -1 because assumes ending with newline
      tokenizer.limit=tokenizer.limit-1;
      madvise(file.mapping,file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   }

   struct QueryEntry {
      bool ignore; // Used for testing
      const char* result;
      uint32_t size;

      inline void* getQuery() {
         return (void*) (reinterpret_cast<const uint8_t*>(this)+sizeof(QueryEntry));
      }

      inline QueryEntry* getNextEntry() {
         return (QueryEntry*) (reinterpret_cast<uint8_t*>(this)+sizeof(QueryEntry)+size);
      }
   };

   struct QueryBatch {
      static const unsigned int batchSpace = 4096;

      uint32_t remaining;
      uint32_t count;
      uint32_t queryType;
      QueryEntry* entries;
      QueryEntry* end;
      QueryEntry* nextInsert;

      QueryBatch(uint32_t type) : remaining(batchSpace), count(0), queryType(type), entries(reinterpret_cast<QueryEntry*>(new uint8_t[batchSpace])), end(entries), nextInsert(entries) {
      }

      QueryBatch(const QueryEntry&) = delete;
      QueryBatch(QueryEntry&&) = delete;

      ~QueryBatch() {
         delete[] reinterpret_cast<uint8_t*>(entries);
      }
   };

   class QueryBatcher {
      static const int numQueryTypes=4;


      QueryParser& parser;

      array<QueryBatch*,numQueryTypes> currentBatch;
      vector<QueryEntry*> queries;

      unordered_set<awfy::StringRef> usedTags; //Tags that are used in queries of type 4

   public:
      array<vector<QueryBatch*>,numQueryTypes> batches;
      array<size_t,numQueryTypes> batchCounts;
      array<size_t,numQueryTypes> batchAssignments;
      array<bool,numQueryTypes> activeTypes;

      bool finished_;


      QueryBatcher(QueryParser& parser) : parser(parser), finished_(false) {
         for (int i = 0; i < numQueryTypes; ++i) {
            currentBatch[i] = new QueryBatch(i);
            batchCounts[i] = 0;
            batchAssignments[i] = 0;
            activeTypes[i]=false;
         }
      }

      ~QueryBatcher() {
         for (int i = 0; i < numQueryTypes; ++i) {
            for(auto batchIter=batches[i].begin(); batchIter!=batches[i].end(); batchIter++) {
               delete *batchIter;
            }
         }
      }

      void parse() {
         vector<uint8_t> queryBuffer(queryfiles::QueryParser::maxQuerySize());

         // Read until end
         while(true) {
            auto query = reinterpret_cast<queryfiles::QueryParser::BaseQuery*>(queryBuffer.data());
            const auto len=parser.readNext(query);
            const auto requiredSpace=len+sizeof(QueryEntry);
            const auto queryType=query->id-'1';
            if(len<0) {
               break;
            }

            assert(queryType>=0);
            assert(queryType<numQueryTypes);
            assert(requiredSpace<QueryBatch::batchSpace);

            activeTypes[queryType]=true;
            QueryBatch* target=currentBatch[queryType];
            unsigned batchLimit=0;
            if(queryType==0) { batchLimit=200; }
            if(queryType==1) { batchLimit=1; }
            if(queryType==2) { batchLimit=1; }
            if(queryType==3) { batchLimit=1; }

            // Check if it still fits, replace otherwise
            if(target->remaining<requiredSpace||target->count==batchLimit) {
               // Stash full targe
               target->end=target->nextInsert;
               batches[queryType].push_back(target);
               batchCounts[queryType]++;
               // Allocate new target
               currentBatch[queryType]=new QueryBatch(queryType);
               target=currentBatch[queryType];
            }

            // Copy query data into entry
            QueryEntry* entry=target->nextInsert;
            entry->size=len;
            entry->ignore=false;
            entry->result=nullptr;
            memcpy(entry->getQuery(),query,len);
            target->nextInsert=entry->getNextEntry();
            target->remaining-=requiredSpace;
            target->count++;
            // Insert into ordered list of queries
            queries.push_back(entry);


            if(queryType==3) {
               //Save tag reference
               auto q4 = reinterpret_cast<QueryParser::Query4*>(entry->getQuery());
               usedTags.insert(awfy::StringRef(q4->getTag(),entry->size-sizeof(QueryParser::Query4)-1));
            }
         }

         // Close all current batches
         for (int i = 0; i < numQueryTypes; ++i) {
            auto target=currentBatch[i];
            if(target->count>0) { // Don't create empty targets
               target->end=target->nextInsert;
               batches[i].push_back(target);
               batchCounts[i]++;
            }
         }

         #ifdef DEBUG
         int i=0;
         for(auto countIter=batchCounts.cbegin(); countIter!=batchCounts.cend(); countIter++, i++) {
            LOG_PRINT("[Queries] Read "<< (*countIter) << " of type "<<i);
         }
         #endif

         finished_=true;
      }

      /// Try to retrieve next batch. Might return nullptr even if parsing is not finished yet.
      /// Has to be retried until finished() returns true;
      QueryBatch* tryGetBatch() {
         // First process large query ids than smaller ones
         for (int i = numQueryTypes-1; i>=0; i--) {
            while(true) {
               const auto nextAssign=batchAssignments[i];
               if(nextAssign<batchCounts[i]) {
                  batchAssignments[i] = nextAssign+1;
                  return batches[i][nextAssign];
               } else {
                  break;
               }
            }
         }

         return nullptr;
      }

      vector<QueryBatch*> getBatches(uint32_t queryType) {
         return batches[queryType];
      }

      array<bool,numQueryTypes> getActiveQueryTypes() {
         return activeTypes;
      }

      vector<QueryEntry*>& getQueryList() {
         return queries;
      }

      bool finished() {
         return finished_;
      }

      const unordered_set<awfy::StringRef>& getUsedTags() const {
         return usedTags;
      }
   };
}

