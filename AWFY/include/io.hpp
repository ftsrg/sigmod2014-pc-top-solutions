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

#include <cstddef>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "macros.hpp"
#include "compatibility.hpp"
#include <vector>
#include <iostream>
using namespace std;

struct SizedBuffer {
   void* data;
   size_t size;

   SizedBuffer() { }
   SizedBuffer(SizedBuffer&& other)
      : data(other.data), size(other.size)
   {
      other.data = nullptr;
   }
};


namespace io {

   /// Owner of a mapped file
   class MmapedFile {
      int fd;
   public:
      size_t size;
      void* mapping;

      MmapedFile(const std::string& path, int flags);
      MmapedFile(const MmapedFile&) = delete;
      MmapedFile(MmapedFile&&);
      ~MmapedFile();
   };

   size_t fileSize(const std::string& path);
   size_t fileLines(const std::string& path);
}
