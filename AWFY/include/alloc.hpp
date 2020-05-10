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

#ifndef GROWONLYALLOCATOR_HPP_
#define GROWONLYALLOCATOR_HPP_

#define NO_CUSTOM_ALLOC

#include "util/log.hpp"
#include "hash.hpp"
#include <memory>
#include <vector>
#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"
#include <set>
#include <list>
#include <utility>
#include <mutex>
#include <functional>
#include <cassert>
#include <iostream>
#ifdef DEBUG
#include <iostream>
#include <thread>
#endif
#include "macros.hpp"
#include "compatibility.hpp"

 #ifdef __GNUC__
#define THREAD_LOCAL __thread
#else
#define THREAD_LOCAL thread_local
#endif

 namespace awfy {

static const size_t defaultChunkSize=16*1024*1024;
static const size_t nonChunkThreshold = 1024*1024; //Allocations bigger than this are not put into the chunks
static_assert(nonChunkThreshold<=defaultChunkSize, "Incorrect relation of chunk size and non-chunk threshold");

#ifdef NO_CUSTOM_ALLOC
class Allocator {
public:
   static Allocator get() {
      return Allocator();
   }

   template<typename T>
   inline T* alloc(size_t size) {
      return reinterpret_cast<T*>(::malloc(size));
   }

   inline void free(void* ptr, __attribute__((unused)) size_t size) __attribute__((always_inline)) {
      ::free(ptr);
   }
};
typedef Allocator AllocatorRef;

template <typename T>
class StdAllocator : public std::allocator<T>
{ };


#else
//-----------------------------
// Custom allocator definition
//-----------------------------

template<typename T> class StdAllocator; //Forward declaration

class Allocator {
   template<typename T> friend class StdAllocator;
private:
   char* pos;
   char* end;

   static THREAD_LOCAL Allocator* threadAllocator;

public:
   static Allocator& get() {
      if(threadAllocator==nullptr) {
         threadAllocator=new Allocator();
      }
      return *threadAllocator;
   }

   Allocator()
      : pos((char*)malloc(defaultChunkSize)), end(pos+defaultChunkSize)
   {
      if(unlikely(pos==nullptr)) {
         FATAL_ERROR("Out of memory Allocator");
      }
   }

   ~Allocator() {
   }

   Allocator(const Allocator&)=delete;
   Allocator& operator=(const Allocator&)=delete;

   template<typename T>
   T* alloc(size_t size) {
      if(size >= nonChunkThreshold) {
         T* ptr;
         const int allocResult = posix_memalign(reinterpret_cast<void**>(&ptr),64,size);
         if(unlikely(allocResult==0)) {
            FATAL_ERROR("Out of memory Allocator alloc("<<size<<") memalign");
         }
         return ptr;
      }

      assert(this == &get()); //We are in the allocator for this thread

      if(unlikely(pos + size > end)) {
         pos=(char*)malloc(defaultChunkSize);
         if(pos==nullptr) {
            FATAL_ERROR("Out of memory Allocator alloc("<<size<<") malloc");
         }
         end=pos+defaultChunkSize;
      }
      const auto outPtr = pos;
      pos+=size;
      return (T*)outPtr;
   }

   void free(void* ptr, size_t size) {
      if(size >= nonChunkThreshold) {
         ::free(ptr);
      }
   }
};

#endif

template<size_t chunkSize=defaultChunkSize>
class BulkFreeAllocator {
private:
   char* pos;
   char* end;
   std::vector<char*> chunks;

public:
   BulkFreeAllocator()
      : pos((char*)malloc(chunkSize)), end(pos+chunkSize)
   {
      if(unlikely(pos==nullptr)) {
         FATAL_ERROR("Out of memory BulkFreeAllocator");
      }
      chunks.push_back(pos);
   }

   ~BulkFreeAllocator() {
      assert(pos!=nullptr);
      for(auto cIter=chunks.begin(); cIter!=chunks.end(); cIter++) {
         ::free(*cIter);
      }
   }

   BulkFreeAllocator(const BulkFreeAllocator&)=delete;
   BulkFreeAllocator& operator=(const BulkFreeAllocator&)=delete;

   BulkFreeAllocator(BulkFreeAllocator&& other)
      : pos(other.pos), end(other.end), chunks(move(other.chunks))
   {
      other.pos=nullptr;
   }
   BulkFreeAllocator& operator=(BulkFreeAllocator&& other) {
      this->pos = other.pos;
      this->end = other.end;
      this->chunks = move(other.chunks);
      other.pos = nullptr;
   }

   template<typename T>
   T* alloc(size_t size) {
      if(unlikely(pos + size > end)) {
         pos=(char*)malloc(chunkSize);
         if(unlikely(pos==nullptr)) {
            FATAL_ERROR("Out of memory BulkFreeAllocator alloc("<<size<<")");
         }
         chunks.push_back(pos);
         end=pos+chunkSize;
      }
      const auto outPtr = pos;
      pos+=size;
      return (T*)outPtr;
   }
};

#ifndef NO_CUSTOM_ALLOC

template<typename T>
class StdAllocator {
   template<typename U> friend class StdAllocator;
 public:
   typedef size_t size_type;
   typedef T value_type;
   typedef T type;
   typedef T* pointer;
   typedef const T* const_pointer;
   typedef T& reference;
   typedef const T& const_reference;
   typedef ptrdiff_t difference_type;

   template<typename _Tp1>
   struct rebind
   {
      typedef StdAllocator<_Tp1> other;
   };

   StdAllocator()
      : allocator(Allocator::get())
   { }
   StdAllocator(const StdAllocator<T>& a) throw()
      : allocator(a.allocator)
   { }
   template<typename _Tp1>
   StdAllocator(const StdAllocator<_Tp1>& a) throw()
      : allocator(a.allocator)
   { }

   pointer allocate(size_type n, __attribute__((unused)) const void *hint=0) {
      return allocator.alloc<T>(n*sizeof(T));
   };

   void deallocate(pointer p, size_type n) {
      allocator.free(p, n*sizeof(T));
   }

   void construct(pointer p, const T& v) {
      new(p) T(v);
   }
   void construct(pointer p, T&& v) {
      new(p) T(v);
   }
   template<class... _Other> void construct(pointer p, _Other&&... v) {
      new(p) T(v...);
   }

   void destroy(pointer p) {
      p->T::~T();
   }

   size_t max_size() const {
      return std::numeric_limits<size_type>::max();
   }

private:
   Allocator& allocator;
};

typedef Allocator& AllocatorRef;
#endif

#ifndef NO_CUSTOM_ALLOC
template <typename T>
class vector : public std::vector<T, StdAllocator<T>>
{ };
#else
template <typename T>
class vector : public std::vector<T>
{ };
#endif

template <typename Key, class Hash=std::hash<Key>, class Pred=std::equal_to<Key>>
class unordered_set : public boost::unordered_set<Key, Hash, Pred, StdAllocator<Key>>
{ };

template <typename Key, typename T, class Hash=std::hash<Key>, class Pred=std::equal_to<Key>>
class unordered_map : public boost::unordered_map<Key, T, Hash, Pred, StdAllocator<std::pair<const Key, T>>>
{ };

template <typename Key, typename T, class Hash=std::hash<Key>, class Pred=std::equal_to<Key>>
class unordered_multimap : public boost::unordered_multimap<Key, T, Hash, Pred, StdAllocator<std::pair<const Key, T>>>
{ };

template <typename T, class Compare=std::less<T>>
class set : public std::set<T, Compare, StdAllocator<T>>
{ };
}

#endif
