#pragma once

#include <chrono>
#include <iostream>
#include <stdint.h>

namespace measurement {

      uint64_t now();

      void queryStart();
      void finished();

      void print(std::ostream& os);

      static inline uint64_t firstQueryStartedAt = 0;
      static inline uint64_t finishedAt = 0;
}