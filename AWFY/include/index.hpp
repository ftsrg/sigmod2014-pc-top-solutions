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

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <queue>
#include <map>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <utility>
#include <limits>

#include "compatibility.hpp"
#include "concurrent/scheduler.hpp"
#include "tokenize.hpp"
#include "../types.hpp"
#include "alloc.hpp"
#include "StringRef.hpp"
#include "campers/hashtable.hpp"

/// =================================
/// New Index API
/// =================================

template<class SizeType, class EntryType>
class SizedList {
   // Fake pointer for proper size (MAGIC)
   SizeType count;
public:

   typedef SizeType Size;
   typedef EntryType Entry;

   SizedList() {
   }

   /// Reads the size for the list at the pointer position
   inline const Size& size() const {
      return count;
   }

   inline Size& size() {
      return count;
   }

   /// Sets the size for the list at the pointer position
   void setSize(const Size count) {
      this->count = count;
   }

   const Entry* getPtr(const size_t index) const __attribute__ ((pure)) {
      const auto offsetPtr = reinterpret_cast<const uint8_t*>(this) + sizeof(Size) + sizeof(Entry)*index;
      return reinterpret_cast<const Entry*>(offsetPtr);
   }

   Entry* getPtr(const size_t index) __attribute__ ((pure)) {
      const auto offsetPtr = reinterpret_cast<uint8_t*>(this) + sizeof(Size) + sizeof(Entry)*index;
      return reinterpret_cast<Entry*>(offsetPtr);
   }

   const Entry* find(const Entry& entry) const __attribute__ ((pure)) {
      assert(sizeof(Entry)==4); //Only implemented for this case

      const Entry* i=this->getPtr(0);
      const Entry* end=i+this->count;
      if (i+4<=end) {  // SSE path for 32bit entries
         __m128i entryRegister=_mm_set1_epi32(*static_cast<const uint32_t*>(reinterpret_cast<const void*>(&entry)));
         unsigned found=0;
         while (i+4<=end) {
            __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(i));
            found=_mm_movemask_epi8(_mm_cmpeq_epi32(data,entryRegister));
            if (found) {
               found=__builtin_ctz(found);
               assert(*(i+(found>>2))==entry);
               return i+(found>>2);
            }
            i+=4;
         }
      }
      while(i != end) {
         if(*i == entry) {
            return i;
         }
         ++i;
      }
      return nullptr;
   }

   pair<const Entry*,const Entry* const> bounds() const __attribute__ ((pure)) {
      const Entry* i=getPtr(0);
      return make_pair(i,i+count);
   }

   SizedList<Size, Entry>* nextList(Size count) __attribute__ ((pure)) {
      auto offsetPtr = reinterpret_cast<uint8_t*>(this)+sizeof(Size)+sizeof(Entry)*count;
      return reinterpret_cast<SizedList<Size, Entry>*>(offsetPtr);
   }
};

template<class EntryType>
class FixedSizeList {
public:
   typedef uint32_t SizeType;

   static const EntryType emptyEntryIndicator = UINT32_MAX;
   static_assert(sizeof(EntryType)==4, "FixedSizeList expects entry type to be 32bit.");

   template<size_t allocChunkSize>
   static FixedSizeList* create(size_t numElements, awfy::BulkFreeAllocator<allocChunkSize>& allocator) {
      auto obj = allocator.template alloc<FixedSizeList>(sizeof(SizeType) + sizeof(EntryType)*(numElements+1));
      *reinterpret_cast<SizeType*>(obj) = numElements;
      auto list = reinterpret_cast<EntryType*>(reinterpret_cast<SizeType*>(obj)+1);
      reinterpret_cast<EntryType*>(list)[numElements] = emptyEntryIndicator;
      return obj;
   }

   inline SizeType size() const __attribute__ ((pure)) {
      return _size;
   }

   EntryType* begin() __attribute__ ((pure)) {
      return reinterpret_cast<EntryType*>(reinterpret_cast<SizeType*>(this)+1);
   }

   /// Returns pair: erased / list contains further elements
   pair<bool,bool> erase(const EntryType& entry) {
      EntryType* pos = begin();
      EntryType* end = begin()+_size;

      bool stop=false;
      if (likely(sizeof(EntryType)==4&&(pos+16)<end)) {  // SSE path for 32bit entries
         __m128i entryRegister=_mm_set1_epi32(*static_cast<const uint32_t*>(reinterpret_cast<const void*>(&entry)));
         unsigned found=0;
         pos-=4;
         do {
            pos+=4;
            __m128i data=_mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
            found=_mm_movemask_epi8(_mm_cmpeq_epi32(data,entryRegister));
            if (found) {
               found=__builtin_ctz(found);
               stop=true;
               pos+=(found>>2);
               break;
            }
         } while ((pos+16)<end);
         if (!stop) {
            pos+=4;
            while (pos!=end) {
               if (*pos==entry) {
                  stop=true;
                  break;
               }
               ++pos;
            }
         }
      } else {
         while (pos!=end) {
            if (*pos==entry) {
               stop=true;
               break;
            }
            ++pos;
         }
      }

      //Found entry?
      if (stop) {
         assert(*pos==entry);
         if(*(pos+1)==emptyEntryIndicator) {
            //Last item in list
            if(pos==begin()) {
               //No more items
               return make_pair(true,false);
            } else {
               *pos = emptyEntryIndicator;
               return make_pair(true,true);
            }
         } else {
            //Shift list
            do {
               *(pos) = *(pos+1);
               ++pos;
            } while(*pos!=emptyEntryIndicator);
            return make_pair(true,true);
         }
      } else {
         return make_pair(false,true);
      }
   }

   pair<EntryType,bool> pop() {
      auto dataStart = begin();
      EntryType e = *dataStart;
      if(*(dataStart+1)==emptyEntryIndicator) {
         return make_pair(move(e),false);
      } else {
         do {
            *(dataStart) = *(dataStart+1);
            ++dataStart;
         } while(*dataStart!=emptyEntryIndicator);
         return make_pair(move(e),true);
      }
   }

private:
   SizeType _size;
};

template<class SizeType, class EntryType>
class LinkedSizedList {
public:
   typedef SizedList<SizeType,EntryType> ListType;
   typedef EntryType Entry;
   typedef SizeType Size;

   LinkedSizedList()
      : listPtr(nullptr),listEndPtr(nullptr)
   { }

   template<typename Allocator=awfy::Allocator>
   ListType* appendList(SizeType numElements, Allocator& allocator) {
	  ListType** curList=listEndPtr;
      *curList = allocator.template alloc<ListType>(sizeof(SizeType) + sizeof(EntryType)*numElements + sizeof(ListType*));
      (*curList)->setSize(numElements);
      listEndPtr=reinterpret_cast<ListType**>((char*)*curList + sizeof(SizeType) + sizeof(EntryType)*numElements);
      *listEndPtr = nullptr; //Set next pointer to null
      return *curList;
   }

   void appendList(SizeType numElements, char* listMemBlock) {
	  ListType** curList=listEndPtr;
      *curList = reinterpret_cast<ListType*>(listMemBlock);
      (*curList)->setSize(numElements);
      listEndPtr=reinterpret_cast<ListType**>((char*)*curList + sizeof(SizeType) + sizeof(EntryType)*numElements);
      *listEndPtr = nullptr; //Set next pointer to null
   }

   void mergeWith(LinkedSizedList<SizeType,EntryType>* listToMerge) {
	  ListType** curList=listEndPtr;
      *curList = listToMerge->firstList();
      listEndPtr=listToMerge->listEndPtr;
   }

   ListType* firstList() const __attribute__ ((pure)) {
      return listPtr;
   }

   ListType* nextList(ListType* list) const __attribute__ ((pure)) {
      return *reinterpret_cast<ListType**>((char*)list + sizeof(SizeType) + sizeof(EntryType)*list->size());
   }

   template<typename Allocator=awfy::Allocator>
   static LinkedSizedList* create(SizeType numElements, Allocator& allocator) {
      auto list = allocator.template alloc<LinkedSizedList>(sizeof(LinkedSizedList) + sizeof(SizeType) + sizeof(EntryType)*numElements + sizeof(ListType*));
      auto listPtr = reinterpret_cast<ListType*>((char*)list+sizeof(LinkedSizedList));
      list->listPtr = listPtr;
      listPtr->setSize(numElements);
      list->listEndPtr=reinterpret_cast<ListType**>((char*)list + sizeof(LinkedSizedList) + sizeof(SizeType) + sizeof(EntryType)*numElements);
      *(list->listEndPtr)=nullptr; //Set next pointer to null
      return list;
   }

   //Creates list from already filled memory block. Sets size and first block ptr.
   static void create(SizeType numElements, char* listMemBlock) {
      auto list = reinterpret_cast<LinkedSizedList*>(listMemBlock);
      auto listPtr = reinterpret_cast<ListType*>((char*)list+sizeof(LinkedSizedList));
      list->listPtr = listPtr;
      listPtr->setSize(numElements);
      list->listEndPtr=reinterpret_cast<ListType**>((char*)list + sizeof(LinkedSizedList) + sizeof(SizeType) + sizeof(EntryType)*numElements);
      *(list->listEndPtr)=nullptr; //Set next pointer to null
   }

private:
   ListType* listPtr;
   ListType** listEndPtr;
};

template<class Id>
class UniqueIdMapper {
   std::unordered_map<uint64_t,Id> mapping;
   std::unordered_map<Id,uint64_t> invMapping;
   Id nextId;

public:
   #ifdef DEBUG
   bool closed;
   #endif

   UniqueIdMapper()
      : nextId(0)
      #ifdef DEBUG
      ,closed(0)
      #endif
   { }

   Id map(uint64_t original) {
      auto iter = mapping.find(original);
      if(iter==mapping.end()) {
         assert(!closed);
         auto id = nextId++;
         mapping.emplace(original, id);
         invMapping.emplace(id, original);
         return id;
      } else {
         return iter->second;
      }
   }

   Id invert(Id id) const __attribute__ ((pure)) {
      return invMapping.find(id)->second;
   }

   Id count() const __attribute__ ((pure)) {
      return nextId;
   }
};

template<class Id>
class FastUniqueIdMapper {
   std::unordered_map<uint64_t,Id> mapping;
   std::vector<uint64_t> invMapping;
   Id nextId;
   Id numIds;

public:
   #ifdef DEBUG
   bool closed;
   #endif

   FastUniqueIdMapper()
      : nextId(0), numIds(numeric_limits<Id>::max())
      #ifdef DEBUG
      ,closed(0)
      #endif
   { }

   FastUniqueIdMapper(size_t numIds)
      : mapping(numIds), invMapping(numIds), nextId(0), numIds(numIds)
      #ifdef DEBUG
      ,closed(0)
      #endif
   { }

   Id map(uint64_t original) {
      assert(numIds<numeric_limits<Id>::max()); //Initialized using correct constructor

      auto iter = mapping.find(original);
      if(iter==mapping.end()) {
         assert(!closed);
         assert(nextId < numIds);

         mapping.emplace(original, nextId);
         invMapping[nextId] = move(original);
         return nextId++;
      } else {
         return iter->second;
      }
   }

   Id invert(Id id) const __attribute__ ((pure)) {
      assert(numIds<numeric_limits<Id>::max()); //Initialized using correct constructor
      return invMapping[id];
   }

   Id count() const __attribute__ ((pure)) {
      assert(numIds<numeric_limits<Id>::max()); //Initialized using correct constructor
      return nextId;
   }
};

template<class Id>
class IdentityMapper {
   Id _count;

public:
   IdentityMapper() : _count(0) {
   }

   Id map(Id original) {
      if(original>=_count) { _count=original+1; }
      return original;
   }

   Id invert(Id original) const __attribute__ ((pure)) {
      return original;
   }

   Id count() const __attribute__ ((pure)) {
      return _count;
   }
};

template<class Id>
class FastIdentityMapper {
   Id _count;

public:
   #ifdef DEBUG
   bool closed;
   #endif
   
   FastIdentityMapper()
      : _count(numeric_limits<Id>::max())
      #ifdef DEBUG
      ,closed(0)
      #endif
   {
   }
   FastIdentityMapper(size_t numIds)
      : _count(numIds)
      #ifdef DEBUG
      ,closed(0)
      #endif
   { }

   Id map(Id original) __attribute__ ((const)) {
      assert(_count<numeric_limits<Id>::max()); //Initialized using correct constructor
      return original;
   }

   Id invert(Id original) const __attribute__ ((const)) {
      assert(_count<numeric_limits<Id>::max()); //Initialized using correct constructor
      return original;
   }

   Id count() const __attribute__ ((const)) {
      assert(_count<numeric_limits<Id>::max()); //Initialized using correct constructor
      return _count;
   }
};

template<class Id>
class CommentIdMapper {
public:
   CommentIdMapper()
   { }

   inline uint64_t map(uint64_t original) {
      return original*0.10;
   }
};


/// Directly mapping index which uses the id as an offset directly to get the content
template<class IdType, class ContentType>
class DirectIndex {
   static_assert(std::is_pointer<ContentType>::value || std::is_integral<ContentType>::value, "DirectIndex::retrieve returns by value, so ContentType must be integral or a pointer.");

   IdType maxId;
   size_t count;

   template<class T>
   inline typename std::enable_if<std::is_pointer<T>::value, T>::type endMarker() const {
      return nullptr;
   }

   template<class T>
   inline typename std::enable_if<std::is_integral<T>::value, T>::type endMarker() const {
      return std::numeric_limits<T>::max();
   }

public:
   ContentType* data;
   SizedBuffer buffer;
   typedef IdType Id;
   typedef ContentType Content;

   DirectIndex() : maxId(0), count(0), data(nullptr) {
   }

   DirectIndex(size_t size) : maxId(0), count(0), data(nullptr) {
      allocate(size);
   }

   DirectIndex(DirectIndex& other) = delete;

   DirectIndex(DirectIndex&& other) : maxId(other.maxId), count(other.count),
         data(other.data), buffer(move(other.buffer)) {
      other.data=nullptr;
   }

   ~DirectIndex() {
      if(data) {
         deallocate();
      }
   }

   void allocate(size_t count) {
      uint64_t allocSize=count*sizeof(ContentType);
      auto ret=posix_memalign(reinterpret_cast<void**>(&data),64,count*sizeof(ContentType));
      if(unlikely(ret!=0)) {
         throw -1;
      }
      memset(data,0,allocSize);
      this->count = count;
   }

   void deallocate() {
      free(data);
      data = nullptr;
   }

   /// Inserts the data for the specified id into the index
   void insert(Id id, Content content) {
      #ifdef DEBUG
      assert(data!=nullptr);
      assert(id<(Id)count);
      #endif
      data[id] = move(content);
      if(id>maxId) {
         maxId=id;
      }
   }
   /// Retrieves the data for the specified id
   const Content retrieve(Id id) const __attribute__ ((pure)) {
      if(unlikely(id>maxId)) {
         return end();
      } else {
         return data[id];
      }
   }

   inline ContentType end() const {
      return endMarker<ContentType>();
   }

   inline IdType maxKey() const {
      return maxId;
   }
};

/// Looks up the content in an internal HashMap
template<class IdType, class ContentType>
class HashIndex {
   typedef campers::HashMap<IdType,ContentType> IndexHashMap;
   IndexHashMap mapping;
   ContentType _end;

   template<class T>
   inline typename std::enable_if<std::is_pointer<T>::value, T>::type endMarker() const {
      return nullptr;
   }
   template<class T>
   inline typename std::enable_if<std::is_integral<T>::value, T>::type endMarker() const {
      return numeric_limits<T>::max();
   }
   template<class T>
   inline typename std::enable_if<std::is_same<T, awfy::StringRef>::value, T>::type endMarker() const {
      return awfy::StringRef();
   }

public:
   typedef IdType Id;
   typedef ContentType Content;
   SizedBuffer buffer;

   HashIndex() :
      mapping(512), _end(end())
   { }

   HashIndex(size_t size) :
      mapping(size), _end(end())
   { }

   HashIndex(const HashIndex&) = delete;
   HashIndex& operator=(const HashIndex&) = delete;

   HashIndex(HashIndex&&) = delete;
   HashIndex& operator=(HashIndex&&) = delete;

   /// Allocate for the specified size
   void allocate(size_t count) {
      mapping.hintSize(count);
   }

   /// Inserts the data for the specified id into the index
   void insert(Id id, Content content) {
      assert(mapping.count(id)==0);
      mapping.tryInsert(id)[0] = move(content);
   }

   /// Retrieves the data for the specified id
   const Content& retrieve(Id id) const __attribute__ ((pure)) {
      auto value = const_cast<campers::HashMap<IdType,ContentType>&>(mapping).find(id);
      if(likely(value!=nullptr)) {
         return *value;
      } else {
         return _end;
      }
   }

   /// Return number of elements contained in this Index
   inline size_t size() const {
      return mapping.size();
   }

   inline ContentType end() const {
      return endMarker<ContentType>();
   }

   /// Merges two hash indexes. NOTE: size will be invalid after this
   void mergeWith(HashIndex<IdType,ContentType>* indexToMerge) {
      auto& targetEntries = this->mapping.entries;
      auto& mergeEntries = indexToMerge->mapping.entries;
      assert(targetEntries.size() == mergeEntries.size());

      size_t numEntries = targetEntries.size();
      for(size_t a=0; a<numEntries; a++) {
         if(mergeEntries[a] != nullptr) {
            if(targetEntries[a] != nullptr) {
               auto curMergeEntry = mergeEntries[a];
               do {
                  auto curTargetEntry = targetEntries[a];
                  bool found = false;
                  do {
                     if(curTargetEntry->word == curMergeEntry->word) {
                        assert(curTargetEntry->hashValue == curMergeEntry->hashValue);
                        //Entry exists in both indexes. Merge
                        curTargetEntry->value->mergeWith(curMergeEntry->value);
                        found=true;
                        break;
                     }
                     curTargetEntry = curTargetEntry->next;
                  } while(curTargetEntry!=nullptr);

                  if(!found) {
                     //Entry only exists in index to merge. Add it
                     const auto formerNext = targetEntries[a]->next;
                     const auto nextMergeEntry = curMergeEntry->next;
                     targetEntries[a]->next = curMergeEntry;
                     curMergeEntry->next = formerNext;
                     curMergeEntry = nextMergeEntry;
                  } else {
                     curMergeEntry = curMergeEntry->next;
                  }
               } while(curMergeEntry!=nullptr);

            } else {
               targetEntries[a] = mergeEntries[a];
            }
         }
      }
   }
};
