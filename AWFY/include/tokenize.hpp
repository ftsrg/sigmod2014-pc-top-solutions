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

#include <cstdint>
#ifdef __SSE2__
#include <emmintrin.h>
#include <pmmintrin.h>
#endif
#include <cassert>
#include "io.hpp"
#include "macros.hpp"
#include <iostream>

using namespace std;

namespace tokenize {
   struct SearchCastLongLongResult {
      uint32_t length;
      int64_t long1;
      int64_t long2;
   };

   static inline int64_t castStringInteger(const char* str,uint32_t strLen);
   static inline uint64_t countLines(const char* iter,const char* limit);
   static inline pair<uint32_t,int64_t> searchCastLong(const char* iter,const char* /*limit*/,const __m128i* sep);
   static inline SearchCastLongLongResult searchCastLongLongSingleDelimiter(const char* iter,const char* /*limit*/,char sc);
   static inline SearchCastLongLongResult searchCastLongLongDistinctDelimiterCacheFirst(const char* iter,const char* /*limit*/,char sc1,char sc2,uint32_t& cachedLength,char (&cachedString)[15],int64_t& cachedValue);
   static inline uint32_t castBirthday(const char* iter,const char* /*limit*/);

   union uint16_2_t {
      uint16_t i16[4];
      uint32_t i32;
   };
   
   union SSE16i8 {
      __m128i reg;
      char i8[16];
   };

   /// Super fast tokenizer for text files
   class Tokenizer {
      const char* iter;
   public:
      const char* limit;
      uint32_t cachedLength;
      char cachedString[15];
      int64_t cachedValue;

      Tokenizer(const char* iter,const size_t limit) : iter(iter), limit((const_cast<char*>(iter)+limit)), cachedLength(0), cachedValue(0)
      {
      }

      Tokenizer(io::MmapedFile& file) : iter(reinterpret_cast<char*>(file.mapping)), limit(iter+file.size), cachedLength(0), cachedValue(0)
      {
      }

      Tokenizer(io::MmapedFile& file, size_t pos) : iter(reinterpret_cast<char*>(file.mapping)+pos), limit(reinterpret_cast<char*>(file.mapping)+file.size), cachedLength(0), cachedValue(0)
      {
      }

      Tokenizer(io::MmapedFile& file, const char* iter) : iter(iter), limit(reinterpret_cast<char*>(file.mapping)+file.size), cachedLength(0), cachedValue(0)
      {
         assert(iter >= reinterpret_cast<const char*>(file.mapping));
         assert(iter < reinterpret_cast<const char*>(file.mapping)+file.size);
      }

      /// Read a long and updates the internal iterator
      int64_t consumeLong(char delimiter) __attribute__ ((warn_unused_result)) {
         const __m128i separator=_mm_set1_epi8(delimiter);
         auto result=searchCastLong(iter, limit, &separator);
         iter+=result.first+1;
         return result.second;
      }

      /// Read two longs and updates the internal iterator
      pair<int64_t,int64_t> consumeLongLongDistinctDelimiterCacheFirst(char delimiter1,char delimiter2) __attribute__ ((warn_unused_result)) {
         SearchCastLongLongResult result=searchCastLongLongDistinctDelimiterCacheFirst(iter,limit,delimiter1,delimiter2,cachedLength,cachedString,cachedValue);
         iter+=result.length+1;
         return make_pair(result.long1,result.long2);
      }

      /// Read two longs and updates the internal iterator
      pair<int64_t,int64_t> consumeLongLongDistinctDelimiter(char sc1,char sc2) __attribute__ ((warn_unused_result)) {
         assert(sc1!=sc2);
         const __m128i separator1=_mm_set1_epi8(sc1);
         const __m128i separator2=_mm_set1_epi8(sc2);
         __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(iter));
         unsigned mask1=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator1));
         unsigned mask2=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator2));
         if (unlikely(!mask1||!mask2)) { // slow code path if we actually read very large numbers
            if (likely(mask1)) {
               const unsigned found1=__builtin_ctz(mask1);
               SSE16i8 subZero;
               subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
               int64_t res1;
               switch (found1) {
                  case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
                  case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
                  case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
                  case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
                  case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
                  case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
                  case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
                  case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
                  case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
                  case 1:  res1=subZero.i8[0]; break;
                  default: {
                     res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                     for (unsigned pos=10;pos<found1;++pos) {
                        res1=(res1*10)+subZero.i8[pos];
                     }
                     break;
                  }
               }
               iter+=found1+1;
               auto res2=searchCastLong(iter,0,&separator2);
               iter+=res2.first+1;
               return make_pair(res1,res2.second);
            } else {
               auto res1=searchCastLong(iter,0,&separator1);
               iter+=res1.first+1;
               auto res2=searchCastLong(iter,0,&separator2);
               iter+=res2.first+1;
               return make_pair(res1.second,res2.second);
            }
         } else {
            const unsigned found1=__builtin_ctz(mask1);
            const unsigned found2=__builtin_ctz(mask2);
            SSE16i8 subZero;
            subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
            int64_t res1;
            switch (found1) {
               case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
               case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
               case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
               case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
               case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
               case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
               case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
               case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
               case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
               case 1:  res1=subZero.i8[0]; break;
               default: {
                  res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                  for (unsigned pos=10;pos<found1;++pos) {
                     res1=(res1*10)+subZero.i8[pos];
                  }
                  break;
               }
            }
            int64_t res2;
            switch (found2-found1-1) {
               case 10: res2=subZero.i8[found1+1]*1000000000L+subZero.i8[found1+2]*100000000L+subZero.i8[found1+3]*10000000L+subZero.i8[found1+4]*1000000L+subZero.i8[found1+5]*100000L+subZero.i8[found1+6]*10000L+subZero.i8[found1+7]*1000L+subZero.i8[found1+8]*100+subZero.i8[found1+9]*10+subZero.i8[found1+10]; break;
               case 9:  res2=subZero.i8[found1+1]*100000000L+subZero.i8[found1+2]*10000000L+subZero.i8[found1+3]*1000000L+subZero.i8[found1+4]*100000L+subZero.i8[found1+5]*10000L+subZero.i8[found1+6]*1000L+subZero.i8[found1+7]*100+subZero.i8[found1+8]*10+subZero.i8[found1+9]; break;
               case 8:  res2=subZero.i8[found1+1]*10000000L+subZero.i8[found1+2]*1000000L+subZero.i8[found1+3]*100000L+subZero.i8[found1+4]*10000L+subZero.i8[found1+5]*1000L+subZero.i8[found1+6]*100+subZero.i8[found1+7]*10+subZero.i8[found1+8]; break;
               case 7:  res2=subZero.i8[found1+1]*1000000L+subZero.i8[found1+2]*100000L+subZero.i8[found1+3]*10000L+subZero.i8[found1+4]*1000L+subZero.i8[found1+5]*100+subZero.i8[found1+6]*10+subZero.i8[found1+7]; break;
               case 6:  res2=subZero.i8[found1+1]*100000L+subZero.i8[found1+2]*10000L+subZero.i8[found1+3]*1000L+subZero.i8[found1+4]*100+subZero.i8[found1+5]*10+subZero.i8[found1+6]; break;
               case 5:  res2=subZero.i8[found1+1]*10000L+subZero.i8[found1+2]*1000L+subZero.i8[found1+3]*100+subZero.i8[found1+4]*10+subZero.i8[found1+5]; break;
               case 4:  res2=subZero.i8[found1+1]*1000L+subZero.i8[found1+2]*100+subZero.i8[found1+3]*10+subZero.i8[found1+4]; break;
               case 3:  res2=subZero.i8[found1+1]*100+subZero.i8[found1+2]*10+subZero.i8[found1+3]; break;
               case 2:  res2=subZero.i8[found1+1]*10+subZero.i8[found1+2]; break;
               case 1:  res2=subZero.i8[found1+1]; break;
               default: {
                  res2=subZero.i8[found1+1]*1000000000L+subZero.i8[found1+2]*100000000L+subZero.i8[found1+3]*10000000L+subZero.i8[found1+4]*1000000L+subZero.i8[found1+5]*100000L+subZero.i8[found1+6]*10000L+subZero.i8[found1+7]*1000L+subZero.i8[found1+8]*100+subZero.i8[found1+9]*10+subZero.i8[found1+10];
                  for (unsigned pos=found1+11;pos<found2;++pos) {
                     res2=(res2*10)+subZero.i8[pos];
                  }
                  break;
               }
            }
            iter+=found2+1;
            return make_pair(res1,res2);
         }
      }

      /// Read two longs and updates the internal iterator
      pair<int64_t,int64_t> consumeLongLongSingleDelimiterCacheFirst(char sc) __attribute__ ((warn_unused_result)) {
         const __m128i separator=_mm_set1_epi8(sc);
         __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(iter));
         unsigned mask=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
         const unsigned popcount=__builtin_popcount(mask);
         if (unlikely(popcount<2)) { // slow code path if we actually read very large numbers
            if (likely(popcount==1)) {
               const unsigned found1=__builtin_ctz(mask);
               SSE16i8 subZero;
               subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
               int64_t res1;
               if (found1==cachedLength&&(memcmp(iter,cachedString,cachedLength)==0)) {
                  res1=cachedValue;
               } else {
                  switch (found1) {
                     case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
                     case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
                     case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
                     case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
                     case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
                     case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
                     case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
                     case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
                     case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
                     case 1:  res1=subZero.i8[0]; break;
                     default: {
                        res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                        for (unsigned pos=10;pos<found1;++pos) {
                           res1=(res1*10)+subZero.i8[pos];
                        }
                        break;
                     }
                  }
                  cachedLength=found1;
                  cachedValue=res1;
                  memcpy(cachedString,iter,found1);
               }
               iter+=found1+1;
               auto res2=searchCastLong(iter,0,&separator);
               iter+=res2.first+1;
               return make_pair(res1,res2.second);
            } else {
               auto res1=searchCastLong(iter,0,&separator);
               iter+=res1.first+1;
               auto res2=searchCastLong(iter,0,&separator);
               iter+=res2.first+1;
               return make_pair(res1.second,res2.second);
            }
         } else {
            const unsigned found1=__builtin_ctz(mask);
            const unsigned found2=__builtin_ctz(mask&(mask-1) /* remove least significant bit from mask */);
            SSE16i8 subZero;
            subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
            int64_t res1;
            if (found1==cachedLength&&(memcmp(iter,cachedString,cachedLength)==0)) {
               res1=cachedValue;
            } else {
               switch (found1) {
                  case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
                  case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
                  case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
                  case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
                  case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
                  case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
                  case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
                  case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
                  case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
                  case 1:  res1=subZero.i8[0]; break;
                  default: {
                     res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                     for (unsigned pos=10;pos<found1;++pos) {
                        res1=(res1*10)+subZero.i8[pos];
                     }
                     break;
                  }
               }
               cachedLength=found1;
               cachedValue=res1;
               memcpy(cachedString,iter,found1);
            }
            int64_t res2;
            switch (found2-found1-1) {
               case 10: res2=subZero.i8[found1+1]*1000000000L+subZero.i8[found1+2]*100000000L+subZero.i8[found1+3]*10000000L+subZero.i8[found1+4]*1000000L+subZero.i8[found1+5]*100000L+subZero.i8[found1+6]*10000L+subZero.i8[found1+7]*1000L+subZero.i8[found1+8]*100+subZero.i8[found1+9]*10+subZero.i8[found1+10]; break;
               case 9:  res2=subZero.i8[found1+1]*100000000L+subZero.i8[found1+2]*10000000L+subZero.i8[found1+3]*1000000L+subZero.i8[found1+4]*100000L+subZero.i8[found1+5]*10000L+subZero.i8[found1+6]*1000L+subZero.i8[found1+7]*100+subZero.i8[found1+8]*10+subZero.i8[found1+9]; break;
               case 8:  res2=subZero.i8[found1+1]*10000000L+subZero.i8[found1+2]*1000000L+subZero.i8[found1+3]*100000L+subZero.i8[found1+4]*10000L+subZero.i8[found1+5]*1000L+subZero.i8[found1+6]*100+subZero.i8[found1+7]*10+subZero.i8[found1+8]; break;
               case 7:  res2=subZero.i8[found1+1]*1000000L+subZero.i8[found1+2]*100000L+subZero.i8[found1+3]*10000L+subZero.i8[found1+4]*1000L+subZero.i8[found1+5]*100+subZero.i8[found1+6]*10+subZero.i8[found1+7]; break;
               case 6:  res2=subZero.i8[found1+1]*100000L+subZero.i8[found1+2]*10000L+subZero.i8[found1+3]*1000L+subZero.i8[found1+4]*100+subZero.i8[found1+5]*10+subZero.i8[found1+6]; break;
               case 5:  res2=subZero.i8[found1+1]*10000L+subZero.i8[found1+2]*1000L+subZero.i8[found1+3]*100+subZero.i8[found1+4]*10+subZero.i8[found1+5]; break;
               case 4:  res2=subZero.i8[found1+1]*1000L+subZero.i8[found1+2]*100+subZero.i8[found1+3]*10+subZero.i8[found1+4]; break;
               case 3:  res2=subZero.i8[found1+1]*100+subZero.i8[found1+2]*10+subZero.i8[found1+3]; break;
               case 2:  res2=subZero.i8[found1+1]*10+subZero.i8[found1+2]; break;
               case 1:  res2=subZero.i8[found1+1]; break;
               default: {
                  res2=subZero.i8[found1+1]*1000000000L+subZero.i8[found1+2]*100000000L+subZero.i8[found1+3]*10000000L+subZero.i8[found1+4]*1000000L+subZero.i8[found1+5]*100000L+subZero.i8[found1+6]*10000L+subZero.i8[found1+7]*1000L+subZero.i8[found1+8]*100+subZero.i8[found1+9]*10+subZero.i8[found1+10];
                  for (unsigned pos=found1+11;pos<found2;++pos) {
                     res2=(res2*10)+subZero.i8[pos];
                  }
                  break;
               }
            }
            iter+=found2+1;
            return make_pair(res1,res2);
         }
      }

      /// Read two longs and updates the internal iterator
      pair<int64_t,int64_t> consumeLongLongSingleDelimiter(char delimiter) __attribute__ ((warn_unused_result)) {
         SearchCastLongLongResult result=searchCastLongLongSingleDelimiter(iter,limit,delimiter);
         iter+=result.length+1;
         return make_pair(result.long1,result.long2);
      }

      /// Read a long width the specified length and afterwards add skipLength after the reading position
      int64_t consumeLongChars(size_t length, size_t iterAdd=0) __attribute__ ((warn_unused_result)) {
         auto result = castStringInteger(iter,length);
         iter+=length+iterAdd;
         return result;
      }

      uint32_t consumeBirthday() __attribute__ ((warn_unused_result)) {
         uint32_t result=castBirthday(iter,limit);
         iter+=10;
         return result;
      }

      /// Skips until past the delimiter (returns the number of bytes skipped)
      void skipAfter(char sc) {
         const __m128i separator=_mm_set1_epi8(sc);
         iter-=16;
         unsigned found=0;
         do {
            iter+=16;
            __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(iter));
            found=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
         } while (!found);
         found=__builtin_ctz(found);
         iter+=found+1;
      }

      uint32_t skipAfterAndCount(char sc) {
         const auto begin=iter;
         const __m128i separator=_mm_set1_epi8(sc);
         iter-=16;
         unsigned found=0;
         do {
            iter+=16;
            __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(iter));
            found=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
         } while (!found);
         found=__builtin_ctz(found);
         iter+=found+1;
         return iter-begin;
      }

      /// Skips the specified amount of bytes
      void skip(size_t length) {
         iter+=length;
      }

      /// Returns the number of lines without updating the internal iterator
      uint64_t countLines() const __attribute__ ((warn_unused_result)) __attribute__ ((pure)) {
         return tokenize::countLines(iter, limit);
      }

      const char* getPositionPtr() __attribute__ ((pure)) {
         return iter;
      }
      void* getPositionPtrVoid() __attribute__ ((pure)) {
         return const_cast<void*>(reinterpret_cast<const void*>(iter));
      }

      void setPositionPtr(const char* ptr) {
         assert(ptr < limit);
         iter = ptr;
      }

      bool finished() const __attribute__ ((pure)) {
         return iter>=limit;
      }
   };

   static inline int64_t castStringInteger(const char* str,uint32_t strLen)
   {
      auto iter=str,limit=str+strLen;
      bool neg=false;
      if (unlikely((*iter)=='-')) {
         neg=true;
         ++iter;
      }
      int64_t result=0;
      for (;iter!=limit;++iter)
         result=(result*10)+((*iter)-'0');
      if (unlikely(neg))
         return -result;
      else
         return result;
   }

   #ifdef __SSE2__
   static inline uint64_t countLines(const char* iter,const char* limit)
   {
      const char* it=iter;
      const __m128i separator=_mm_set1_epi8('\n');
      uint64_t lines=0;
      /// We iterate over mmapped pages (which are a multiple of 64B); thus we can use aligned loads and do not have to check if we read too much
      if (it+32<limit) {
         do {
            uint16_2_t a;
            __m128i data1=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
            __m128i data2=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it+16));
            a.i16[0]=_mm_movemask_epi8(_mm_cmpeq_epi8(data1,separator));
            a.i16[1]=_mm_movemask_epi8(_mm_cmpeq_epi8(data2,separator));
            lines+=__builtin_popcount(a.i32);
            it+=32;
         } while (it<limit);
      }
      if (it<limit) {
         do {
            if(*it=='\n') { lines++; }
            ++it;
         } while (it<limit);
         if(*(it-1)!='\n') { lines++; }
      }
      return lines;
   }

   static inline uint32_t castBirthday(const char* iter,const char* /*limit*/)
   {
      auto it=iter;
      __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
      SSE16i8 subZero;
      subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
      uint32_t birthday=0;
      birthday=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3];
      birthday<<=8;
      birthday+=subZero.i8[5]*10+subZero.i8[6];
      birthday<<=8;
      birthday+=subZero.i8[8]*10+subZero.i8[9];
      return birthday;
   }

   static inline pair<uint32_t,int64_t> searchCastLong(const char* iter,const char* /*limit*/,const __m128i* sep)
   {
      auto it=iter;
      const __m128i& separator=*sep;
      __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
      unsigned mask=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
      SSE16i8 subZero;
      subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
      if (unlikely(!mask)) {
         int64_t result=0;
         result=subZero.i8[0]*10000000000000l+subZero.i8[1]*1000000000000l+subZero.i8[2]*100000000000l+subZero.i8[3]*10000000000l+subZero.i8[4]*1000000000l+subZero.i8[5]*100000000l+subZero.i8[6]*100000000l+subZero.i8[7]*100000000l+subZero.i8[8]*10000000l+subZero.i8[9]*1000000l+subZero.i8[10]*100000l+subZero.i8[11]*10000l+subZero.i8[12]*1000l+subZero.i8[13]*100l+subZero.i8[14]*10l+subZero.i8[15];
         it+=16;
         data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
         mask=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
         assert(mask!=0); // mask cannot be 0!
         const unsigned found=__builtin_ctz(mask);
         subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
         switch (found) {
            case 10: result=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
            case 9:  result=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
            case 8:  result=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
            case 7:  result=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
            case 6:  result=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
            case 5:  result=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
            case 4:  result=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
            case 3:  result=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
            case 2:  result=subZero.i8[0]*10+subZero.i8[1]; break;
            case 1:  result=subZero.i8[0]; break;
            default: {
               result=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
               for (unsigned pos=10;pos<found;++pos) {
                  result=(result*10)+subZero.i8[pos];
               }
               break;
            }
         }
         return make_pair(found+16,result);
      } else {
         const unsigned found=__builtin_ctz(mask);
         int64_t result=0;
         switch (found) {
            case 10: result=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
            case 9:  result=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
            case 8:  result=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
            case 7:  result=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
            case 6:  result=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
            case 5:  result=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
            case 4:  result=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
            case 3:  result=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
            case 2:  result=subZero.i8[0]*10+subZero.i8[1]; break;
            case 1:  result=subZero.i8[0]; break;
            default: {
               result=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
               for (unsigned pos=10;pos<found;++pos) {
                  result=(result*10)+subZero.i8[pos];
               }
               break;
            }
         }
         return make_pair(found,result);
      }
      assert(false);
   }

   static inline SearchCastLongLongResult searchCastLongLongSingleDelimiter(const char* iter,const char* /*limit*/,char sc)
   {
      SearchCastLongLongResult result;
      auto it=iter;
      const __m128i separator=_mm_set1_epi8(sc);
      __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
      unsigned mask=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator));
      unsigned popcount=__builtin_popcount(mask);
      if (unlikely(popcount<2)) { // slow code path if we actually read very large numbers
         if (likely(popcount==1)) {
            unsigned found1=__builtin_ctz(mask);
            SSE16i8 subZero;
            subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
            int64_t res1=0;
            switch (found1) {
               case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
               case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
               case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
               case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
               case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
               case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
               case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
               case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
               case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
               case 1:  res1=subZero.i8[0]; break;
               default: {
                  res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                  for (unsigned pos=10;pos<found1;++pos) {
                     res1=(res1*10)+subZero.i8[pos];
                  }
                  break;
               }
            }
            it+=found1+1;
            auto res2=searchCastLong(it,0,&separator);
            result.length=found1+1+res2.first;
            result.long1=res1;
            result.long2=res2.second;
            return result;
         } else {
            auto res1=searchCastLong(it,0,&separator);
            it+=res1.first+1;
            auto res2=searchCastLong(it,0,&separator);
            result.length=res1.first+1+res2.first;
            result.long1=res1.second;
            result.long2=res2.second;
            return result;
         }
      } else {
         unsigned found1=__builtin_ctz(mask);
         mask&=mask-1; // remove least significant bit from mask
         unsigned found2=__builtin_ctz(mask);
         SSE16i8 subZero;
         subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
         int64_t res1=0;
         int64_t res2=0;
         switch (found1) {
            case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
            case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
            case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
            case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
            case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
            case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
            case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
            case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
            case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
            case 1:  res1=subZero.i8[0]; break;
            default: {
               res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
               for (unsigned pos=10;pos<found1;++pos) {
                  res1=(res1*10)+subZero.i8[pos];
               }
               break;
            }
         }
         for (unsigned pos=found1+1;pos<found2;++pos) {
            res2=(res2*10)+subZero.i8[pos];
         }
         result.length=found2;
         result.long1=res1;
         result.long2=res2;
         return result;
      }
   }

   static inline SearchCastLongLongResult searchCastLongLongDistinctDelimiterCacheFirst(const char* iter,const char* /*limit*/,char sc1,char sc2,uint32_t& cachedLength,char (&cachedString)[15],int64_t& cachedValue)
   {
      assert(sc1!=sc2);
      SearchCastLongLongResult result;
      auto it=iter;
      const __m128i separator1=_mm_set1_epi8(sc1);
      const __m128i separator2=_mm_set1_epi8(sc2);
      __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(it));
      unsigned mask1=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator1));
      unsigned mask2=_mm_movemask_epi8(_mm_cmpeq_epi8(data,separator2));
      if (unlikely(!mask1||!mask2)) { // slow code path if we actually read very large numbers
         if (likely(mask1)) {
            unsigned found1=__builtin_ctz(mask1);
            SSE16i8 subZero;
            subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
            int64_t res1=0;
            if (found1==cachedLength&&(memcmp(it,cachedString,cachedLength)==0)) {
               res1=cachedValue;
            } else {
               switch (found1) {
                  case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
                  case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
                  case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
                  case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
                  case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
                  case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
                  case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
                  case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
                  case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
                  case 1:  res1=subZero.i8[0]; break;
                  default: {
                     res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                     for (unsigned pos=10;pos<found1;++pos) {
                        res1=(res1*10)+subZero.i8[pos];
                     }
                     break;
                  }
               }
               cachedLength=found1;
               cachedValue=res1;
               memcpy(cachedString,it,found1);
            }
            it+=found1+1;
            auto res2=searchCastLong(it,0,&separator2);
            result.length=found1+1+res2.first;
            result.long1=res1;
            result.long2=res2.second;
            return result;
         } else {
            auto res1=searchCastLong(it,0,&separator1);
            it+=res1.first+1;
            auto res2=searchCastLong(it,0,&separator2);
            result.length=res1.first+1+res2.first;
            result.long1=res1.second;
            result.long2=res2.second;
            return result;
         }
      } else {
         unsigned found1=__builtin_ctz(mask1);
         unsigned found2=__builtin_ctz(mask2);
         SSE16i8 subZero;
         subZero.reg=_mm_sub_epi8(data,_mm_set1_epi8('0'));
         int64_t res1=0;
         int64_t res2=0;
         if (found1==cachedLength&&(memcmp(it,cachedString,cachedLength)==0)) {
            res1=cachedValue;
         } else {
            switch (found1) {
               case 10: res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9]; break;
               case 9:  res1=subZero.i8[0]*100000000L+subZero.i8[1]*10000000L+subZero.i8[2]*1000000L+subZero.i8[3]*100000L+subZero.i8[4]*10000L+subZero.i8[5]*1000L+subZero.i8[6]*100+subZero.i8[7]*10+subZero.i8[8]; break;
               case 8:  res1=subZero.i8[0]*10000000L+subZero.i8[1]*1000000L+subZero.i8[2]*100000L+subZero.i8[3]*10000L+subZero.i8[4]*1000L+subZero.i8[5]*100+subZero.i8[6]*10+subZero.i8[7]; break;
               case 7:  res1=subZero.i8[0]*1000000L+subZero.i8[1]*100000L+subZero.i8[2]*10000L+subZero.i8[3]*1000L+subZero.i8[4]*100+subZero.i8[5]*10+subZero.i8[6]; break;
               case 6:  res1=subZero.i8[0]*100000L+subZero.i8[1]*10000L+subZero.i8[2]*1000L+subZero.i8[3]*100+subZero.i8[4]*10+subZero.i8[5]; break;
               case 5:  res1=subZero.i8[0]*10000L+subZero.i8[1]*1000L+subZero.i8[2]*100+subZero.i8[3]*10+subZero.i8[4]; break;
               case 4:  res1=subZero.i8[0]*1000L+subZero.i8[1]*100+subZero.i8[2]*10+subZero.i8[3]; break;
               case 3:  res1=subZero.i8[0]*100+subZero.i8[1]*10+subZero.i8[2]; break;
               case 2:  res1=subZero.i8[0]*10+subZero.i8[1]; break;
               case 1:  res1=subZero.i8[0]; break;
               default: {
                  res1=subZero.i8[0]*1000000000L+subZero.i8[1]*100000000L+subZero.i8[2]*10000000L+subZero.i8[3]*1000000L+subZero.i8[4]*100000L+subZero.i8[5]*10000L+subZero.i8[6]*1000L+subZero.i8[7]*100+subZero.i8[8]*10+subZero.i8[9];
                  for (unsigned pos=10;pos<found1;++pos) {
                     res1=(res1*10)+subZero.i8[pos];
                  }
                  break;
               }
            }
            cachedLength=found1;
            cachedValue=res1;
            memcpy(cachedString,it,found1);
         }
         for (unsigned pos=found1+1;pos<found2;++pos) {
            res2=(res2*10)+subZero.i8[pos];
         }
         result.length=found2;
         result.long1=res1;
         result.long2=res2;
         return result;
      }
   }
   #endif
}

