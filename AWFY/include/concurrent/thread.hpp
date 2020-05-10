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
#include <assert.h>
#include <pthread.h>

#include "mutex.hpp"

namespace awfy {

template<class Runnable>
class Thread {
   Runnable* runnable;
   pthread_t tid;
   bool joined;

public:
   Thread(void *(*start_routine)(void*), Runnable* runnable) : runnable(runnable), joined(false) {
      if (pthread_create(&tid,nullptr,start_routine,runnable)) {
         FATAL_ERROR("Could not create thread");
      }
   }

   ~Thread() {
      assert(joined);
      delete runnable;
   }

   Thread(const Thread& other) = delete;
   Thread(Thread&& other) : runnable(other.runnable), tid(other.tid), joined(other.joined) {
      other.runnable = nullptr;
      other.tid = -1;
      other.joined = true;
   }

   void join() {
      assert(!joined);
      void* result;
      pthread_join(tid,&result);
      joined=true;   
   }
};

}
