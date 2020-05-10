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

#include <pthread.h>
#include "../compatibility.hpp"

namespace awfy {
class Mutex
{
   private:
   pthread_mutex_t mutex;

   public:
   Mutex()
   {
      pthread_mutex_init(&mutex,nullptr);
   }
   ~Mutex()
   {
      pthread_mutex_destroy(&mutex);
   }
   void lock()
   {
      pthread_mutex_lock(&mutex);
   }
   void unlock()
   {
      pthread_mutex_unlock(&mutex);
   }
   pthread_mutex_t* get()
   {
      return &mutex;
   }
};

class Condition
{
   private:
   pthread_cond_t condition;

   public:
   Condition()
   {
      pthread_cond_init(&condition,nullptr);
   }
   ~Condition()
   {
      pthread_cond_destroy(&condition);
   }
   void wait(pthread_mutex_t* mutex)
   {
      pthread_cond_wait(&condition,mutex);
   }
   void signal()
   {
      pthread_cond_signal(&condition);
   }
   void broadcast()
   {
      pthread_cond_broadcast(&condition);
   }
};

template<class Lock>
class lock_guard {
private:
   Lock& lock;

public:
   lock_guard(Lock& lock) : lock(lock) {
      lock.lock();
   }

   ~lock_guard() {
      lock.unlock();
   }
};
}
