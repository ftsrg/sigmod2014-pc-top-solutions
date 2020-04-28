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

#include "../include/util/memoryhooks.hpp"
#include "../include/compatibility.hpp"
#include <sys/mman.h>

extern "C" {
   /// Provided by glibc
   extern void *__real_malloc(size_t size) throw();
   extern void *__real_mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
   extern int __real_posix_memalign(void **memptr, size_t alignment, size_t size);

   /* This function wraps the real malloc */
   void* __wrap_malloc (size_t size) throw() {
      awfy::memoryhooks::malloc_hook(size);
      return __real_malloc(size);
   }

   /* This function wraps the real mmap */
   void *__wrap_mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
      if((flags&MAP_ANONYMOUS)==MAP_ANONYMOUS) {
         awfy::memoryhooks::malloc_hook(length);
      }
      return __real_mmap(addr, length, prot, flags, fd, offset);
   }

   /* This function wraps the real posix_memalign */
   int __wrap_posix_memalign(void **memptr, size_t alignment, size_t size) {
      awfy::memoryhooks::malloc_hook(size);
      return __real_posix_memalign(memptr, alignment, size);
   }
}

namespace awfy {
   namespace memoryhooks {
      __thread void (*CurrentThread::report_fn)(size_t)=nullptr;
   }
}