/*
Copyright 2020 János Benjamin Antal

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

#include "../include/util/measurement.hpp"

void measurement::queryStart() {
   auto current = awfy::chrono::now();
   if (firstQueryStartedAt > 0) {
      return;
   }
   firstQueryStartedAt = current;
}

void measurement::finished() {
   auto current = awfy::chrono::now();
   if (finishedAt > 0) {
      throw "cannot finish twice";
   }
   finishedAt = current;
}


void measurement::print(std::ostream& os) {
   os << firstQueryStartedAt << ',' << finishedAt - firstQueryStartedAt;
}