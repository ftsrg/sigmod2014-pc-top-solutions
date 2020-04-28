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

#include <vector>
#include <string>

namespace awfy {

   template<class T>
   class Queue {
   private:

      T* elems;
      size_t size_;
      size_t size_M1;
      size_t count;
      size_t start;
      size_t end;

      void grow() {
         // grow by a factor of 4
         const size_t newSize = size_*4;
         T* newElems=new T[newSize];
         memcpy(newElems, elems, sizeof(T)*size_);
         delete[] elems;

         // Fix order of elements, as size is always 4x we always will have enough space to copy
         if(end<start) {
            memcpy(newElems+size_, newElems, end*sizeof(T));
            end=size_+end;
         }

         elems=newElems;
         size_=newSize;
         size_M1=newSize-1;
      }

      public:
         Queue(size_t reserveSize) : elems(new T[reserveSize]),size_(reserveSize),size_M1(reserveSize-1),count(0),start(0),end(0)
         {  }

         Queue() : elems(new T[8]),size_(8),size_M1(7),count(0),start(0),end(0)
         {  }

         ~Queue() {
            if(elems!=nullptr) {
               delete[] elems;
            }
         }

         inline bool empty() const {
            return count==0;
         }

         inline size_t size() const {
            return count;
         }

         inline const T& front() const __attribute__((always_inline)) {
            assert(!empty());
            return elems[start];
         }

         void pop_front() {
            assert(!empty());

            start++;
            count--;
            if(start==size_) {
               start=0;
            }
         }

         T& push_back_pos() {
            // Check if dequeue has to be resized
            if(unlikely(count==size_M1)) {
               grow();
            }

            // Normal insertion
            assert(end<size_);

            count++;

            // Check if pointers wrap around
            if(size_M1!=end) {
               return elems[end++];
            } else {
               auto& p = elems[end];
               end=0;
               return p;
            }
         }

         void push_back(T val) {
               // Check if dequeue has to be resized
            if(unlikely(count==size_M1)) {
               grow();
            }

            // Normal insertion
            assert(end<size_);

            elems[end]=move(val);
            count++;
            // Check if pointers wrap around
            if(size_M1!=end) {
               end++;
            } else {
               end=0;
            }
         }

      void clear() {
         start=0;
         end=0;
         count=0;
      }
   };

   template<class T>
   class FixedSizeQueue {
   private:
      T* elems;
      T* startPtr;
      T* endPtr;
      size_t size_;

   public:
      FixedSizeQueue(size_t size) : elems(new T[size]),startPtr(elems),endPtr(elems),size_(size)
      {  }

      ~FixedSizeQueue() {
         if(elems!=nullptr) {
            delete[] elems;
         }
      }

      inline bool empty() const {
         return startPtr==endPtr;
      }

      inline size_t size() const {
         return endPtr-startPtr;
      }

      inline const T& front() const {
         assert(!empty());
         return *startPtr;
      }

      inline void pop_front() {
         assert(!empty());
         // assert(start<size_);
         startPtr++;
      }

      inline T& push_back_pos() {
         return *endPtr++;
      }

      void reset(size_t newSize) {
         if(newSize>size_) {
            delete[] elems;
            elems = new T[newSize];
            size_ = newSize;
         }
         startPtr=elems;
         endPtr=elems;
      }

      inline pair<T*,T*> bounds() {
         return make_pair(startPtr,endPtr);
      }
   };

}
