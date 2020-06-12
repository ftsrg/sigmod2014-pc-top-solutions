#include "measurement.h"

uint64_t measurement::now() {
   static const std::chrono::high_resolution_clock::time_point startTime = std::chrono::high_resolution_clock::now();
   auto current=std::chrono::high_resolution_clock::now();
   return std::chrono::duration_cast<std::chrono::microseconds>(current-startTime).count();
}

void measurement::queryStart() {
   auto current = now();
   if (firstQueryStartedAt > 0) {
      return;
   }
   firstQueryStartedAt = current;
}

void measurement::finished() {
   auto current = now();
   if (finishedAt > 0) {
      throw "cannot finish twice";
   }
   finishedAt = current;
}


void measurement::print(std::ostream& os) {
   os << firstQueryStartedAt << ',' << finishedAt - firstQueryStartedAt;
}