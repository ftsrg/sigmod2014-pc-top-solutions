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

#ifndef STRINGREF_HPP
#define STRINGREF_HPP
#include <cstdint>
#include <cstring>
#include <cstddef>
#include <stdexcept>
#include <algorithm>
#include <iterator>
#include <string>
#include <iosfwd>
#include "compatibility.hpp"

namespace awfy {

class StringRef {
   public:
   uint32_t strLen;
   const char* str;

   public:
   StringRef(const char* str,uint32_t strLen)
      : strLen(strLen),str(str)
   {
   }
   StringRef()
      : strLen(0),str(nullptr)
   {
   }
   ~StringRef() {}

   inline bool operator==(const StringRef& other) const
   {
      if (strLen!=other.strLen) return false;
      int cmp=strcmp(str,other.str);
      return (cmp==0);
   }
   inline bool operator!=(const StringRef& other) const
   {
      int cmp=strcmp(str,other.str);
      return (cmp!=0);
   }
   inline bool operator<(const StringRef& other) const
   {
      int cmp=strcmp(str,other.str);
      return (cmp<0);
   }
   inline bool operator>(const StringRef& other) const
   {
      int cmp=strcmp(str,other.str);
      return (cmp>0);
   }
   inline bool operator<=(const StringRef& other) const
   {
      int cmp=strcmp(str,other.str);
      return (cmp<=0);
   }
   inline bool operator>=(const StringRef& other) const
   {
      int cmp=strcmp(str,other.str);
      return (cmp>=0);
   }
};

}
#endif
