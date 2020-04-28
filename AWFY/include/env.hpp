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

#include <string>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>

namespace env {

   /// A very simple command line parser
   class ArgsParser {
      char** begin;
      char** end;

   public:
      ArgsParser(int argc, char** argv);

      const char* getOption(const char* option) const;
      bool existsOption(const char* option) const;

      // Helper function related to environment variables
      template<class T, class F>
      T getOptionAs(const char* option, const T defaultValue, F&& conv) const {
         const char* value = getOption(option);
         if (value) {
            return conv(value, nullptr, 10);
         } else {
            return defaultValue;
         }
      }

      uint32_t getOptionAsUint32(const char* option, const uint32_t defaultValue) const;
   };

   ArgsParser::ArgsParser(int argc, char** argv) :
         begin(argv), end(argv + argc) {
      auto iter = begin;
      std::string args;
      while(iter != end) {
         args.append(*iter);
         args.append(" ");
         iter++;
      }
   }

   const char* ArgsParser::getOption(const char* option) const {
      auto iter = begin;
      while(iter != end) {
         if(strcmp(*iter, option) == 0) {
            if(++iter != end) {
               return *iter;
            } else {
               return nullptr;
            }
         }
         iter++;
      }
      return nullptr;
   }

   bool ArgsParser::existsOption(const char* option) const {
      return getOption(option) != nullptr;
   }

   uint32_t ArgsParser::getOptionAsUint32(const char* option, const uint32_t defaultValue) const {
      return getOptionAs<uint32_t>(option, defaultValue, strtol);
   }

}