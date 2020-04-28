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

namespace awfy{
   template<class Type>
   class atomic {
      Type value;

public:
      atomic() : value(0) {
      }

      atomic(Type initial) : value(initial) {
      }

      atomic(const atomic&) = delete;
      atomic(atomic&&) = delete;

      Type fetch_add(Type operand) {
         return __sync_fetch_and_add(&value, operand);
      }

      Type fetch_or(Type operand) {
         return __sync_fetch_and_or(&value, operand);
      }

      bool compare_exchange_strong(Type expected, Type newValue) {
         return __sync_bool_compare_and_swap(&value, expected, newValue);
      }

      Type load() const {
         __sync_synchronize();
         return value;
      }

      void store(Type operand) {
         value=operand;
      }

      friend std::ostream& operator<<(std::ostream &out, atomic<Type>& v) {
         out<<v.load();
         return out;
      }
   };

// Specialization for double
   union doubleInt {
      double d;
      uint64_t u;
   };

   template<>
   class atomic<double> {
      uint64_t value;

public:
      atomic() : value(0.0) {
      }

      atomic(double initial) : value(*((uint64_t*)(void*)&initial)) {
      }

      atomic(const atomic&) = delete;
      atomic(atomic&&) = delete;

      bool compare_exchange_strong(double expected, double newValue) {
         uint64_t uExpected=*((uint64_t*)(void*)&expected);
         uint64_t uNewValue=*((uint64_t*)(void*)&newValue);
         return __sync_bool_compare_and_swap(&value, uExpected, uNewValue);
      }

      double load() const {
         __sync_synchronize();
         return *((double*)(void*)&value);
      }

      void store(double operand) {
         value=*((uint64_t*)(void*)&operand);
      }

      friend std::ostream& operator<<(std::ostream &out, atomic<double>& v) {
         out<<v.load();
         return out;
      }
   };
}