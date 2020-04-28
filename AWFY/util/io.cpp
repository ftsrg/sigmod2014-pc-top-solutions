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

#include "../include/io.hpp"

#include <string>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include "../include/compatibility.hpp"
#include "../include/tokenize.hpp"
#include "../include/util/log.hpp"

using namespace io;

MmapedFile::MmapedFile(const std::string& path, int flags) {
   fd = ::open(path.c_str(),flags);
   if (unlikely(fd<0)) { FATAL_ERROR("Could not open file. Missing \"/\" at end of path? " << path); }
   size = lseek(fd,0,SEEK_END);
   if (unlikely(!(~size))) { ::close(fd); FATAL_ERROR("Could not get file size. " << path); }
   mapping = mmap(0,size,PROT_READ,MAP_PRIVATE,fd,0);
   if (unlikely(!mapping)) { ::close(fd); FATAL_ERROR("Could not memory map file. " << path); }
}

MmapedFile::MmapedFile(MmapedFile&& other) : fd(other.fd), size(other.size), mapping(other.mapping) {
   other.fd = -1;
   other.size = 0;
   other.mapping = nullptr;
}

MmapedFile::~MmapedFile() {
   if(fd != -1) {
      munmap(mapping, size);
      ::close(fd);
   }
}

size_t io::fileSize(const std::string& path) {
   auto fd = ::open(path.c_str(),O_RDONLY);
   if (unlikely(fd<0)) { FATAL_ERROR("Could not open file. " << path); }
   const size_t size = lseek(fd,0,SEEK_END);
   if (unlikely(!(~size))) { ::close(fd); FATAL_ERROR("Could not get file size. " << path); }
   ::close(fd);
   return size;
}

size_t io::fileLines(const std::string& path) {
   io::MmapedFile file(path, O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);

   return tokenizer.countLines();
}
