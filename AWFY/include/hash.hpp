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
#include <string>
#include <tuple>
#include <functional>
#include <x86intrin.h>
#include "MurmurHash2.h"
#include "MurmurHash3.h"
#include "StringRef.hpp"

const uint32_t seed=0x9f462312;

/// Adds hashing for boost string references to std::hash
namespace std {
template<>
  struct hash<awfy::StringRef> {
    inline size_t operator()(const awfy::StringRef& s) const {
      uint32_t result;
      MurmurHash3_x86_32(s.str,s.strLen,seed,&result);
      return result;
    }
  };
}

namespace boost
{
  inline std::size_t hash_value(const awfy::StringRef& s) {
    uint32_t result;
    MurmurHash3_x86_32(s.str,s.strLen,seed,&result);
    return result;
  }
}

namespace awfy {

  inline uint32_t larsonHash(uint32_t x)
  {
   const char* xArray=reinterpret_cast<const char*>(&x);
   unsigned h=0;
   for (unsigned i=0;i<4;++i)
    h=h*101+(unsigned) *xArray++;
  return h;
}

inline uint32_t larsonHash(const char* x,uint32_t len)
{
 unsigned h=0;
 for (unsigned i=0;i<len;++i)
  h=h*101+(unsigned) *x++;
return h;
}

class DenseHash {
public:
    /// Integers
  inline size_t operator()(uint64_t x) const {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
  }
};

class AWFYHash {
public:
inline size_t operator()(uint64_t x) const {
   uint32_t result;
   MurmurHash3_x86_32(&x,sizeof(x),seed,&result);
   return result;
}

inline size_t operator()(int64_t x) const {
   uint32_t result;
   MurmurHash3_x86_32(&x,sizeof(x),seed,&result);
   return result;
}

inline size_t operator()(uint32_t x) const
{
     return larsonHash(x);
}

inline size_t operator()(int32_t x) const {
     return larsonHash(x);
}

inline size_t operator()(const std::tuple<uint32_t,uint32_t>& t) const {
   uint32_t result;
   MurmurHash3_x86_32(&t,sizeof(t)/2,seed,&result);
   return result;
}

inline size_t operator()(void* ptr) const {
   return std::hash<void*>()(ptr);
}

inline unsigned operator()(const awfy::StringRef& s) const {
   uint32_t result;
   MurmurHash3_x86_32(s.str,s.strLen,seed,&result);
   return result;
}
};

}
