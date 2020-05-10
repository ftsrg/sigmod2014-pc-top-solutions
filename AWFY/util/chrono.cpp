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

#include "../include/util/chrono.hpp"

awfy::chrono::Time awfy::chrono::now() {
   static const std::chrono::high_resolution_clock::time_point startTime = std::chrono::high_resolution_clock::now();
   auto current=std::chrono::high_resolution_clock::now();
   return std::chrono::duration_cast<std::chrono::microseconds>(current-startTime).count();
}

void awfy::chrono::TimeFrame::start() {
   startTime=awfy::chrono::now();
}

void awfy::chrono::TimeFrame::end() {
   endTime=awfy::chrono::now();
   duration=endTime-startTime;
}