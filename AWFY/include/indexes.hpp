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

#include "../types.hpp"
#include "index.hpp"
#include "hash.hpp"
#include "concurrent/scheduler.hpp"
#include "alloc.hpp"
#include "boost/unordered_set.hpp"
#include "StringRef.hpp"
#include "schedulegraph.hpp"

/// ID Mappers
typedef FastIdentityMapper<PersonId> PersonMapper;
typedef CommentIdMapper<uint32_t> CommentMapper;

/// Indexes
typedef DirectIndex<PersonId,SizedList<uint32_t,PersonId>*> PersonGraph;
typedef DirectIndex<CommentId,PersonId> CommentCreatorMap;
typedef const void* PersonCommentedGraph;

inline Birthday encodeBirthday(uint32_t birthYear,uint32_t birthMonth,uint32_t birthDay) {
   return (birthYear<<16)+(birthMonth<<8)+birthDay;
}

struct TagIndex {
   HashIndex<InterestId,awfy::StringRef> idToStr;
   HashIndex<awfy::StringRef,InterestId> strToId;

   boost::unordered_set<InterestId> usedTags;


   TagIndex(size_t expectedNumTags)
      : idToStr(expectedNumTags), strToId(expectedNumTags)
   { }

   TagIndex(const TagIndex&) = delete;
   TagIndex& operator=(const TagIndex&) = delete;

   TagIndex(TagIndex&&) = delete;
   TagIndex& operator=(TagIndex&&) = delete;
};

struct InterestStat {
   InterestId interest;
   uint32_t numPersons;
   Birthday maxBirthday;

   InterestStat(InterestId interest)
      : interest(interest), numPersons(0), maxBirthday(0)
   { }
};

typedef DirectIndex<PersonId,SizedList<uint32_t,InterestId>*> HasInterestIndex;
typedef std::vector<InterestStat> InterestStatistics;

typedef HashIndex<InterestId,LinkedSizedList<uint32_t,ForumId>*> TagInForumsIndex;
struct TagInForums {
   const TagInForumsIndex* index;
   boost::unordered_set<ForumId> forums;

   TagInForums() : index(nullptr) {}
};
typedef HashIndex<ForumId,LinkedSizedList<uint32_t,PersonId>*> HasMemberIndex;

struct PlacesTreeElement {
   vector<PlacesTreeElement*> childElements;
   const PlaceId placeId;

   PlacesTreeElement(PlaceId placeId)
      : placeId(placeId)
   { }
   PlacesTreeElement(PlaceId placeId, vector<PlacesTreeElement*> children)
      : childElements(std::move(children)), placeId(placeId)
   { }
};

struct PlacesTree {
   PlacesTreeElement root;
   boost::unordered_map<PlaceId,PlacesTreeElement> places;

   PlacesTree(PlacesTreeElement root, boost::unordered_map<PlaceId,PlacesTreeElement> places)
      : root(std::move(root)), places(std::move(places))
   { }
};

typedef uint32_t PlaceBound;
struct PlaceBounds {
private:
   typedef uint64_t FastComparisonType;
   static_assert(2*sizeof(PlaceBound)==sizeof(FastComparisonType), "Invalid PlaceBound type.");
   union {
      struct {
         PlaceBound upper;
         PlaceBound lower;
      } data;
      FastComparisonType comparison;
   } bounds;

public:
   PlaceBounds()
   { }

   PlaceBounds(PlaceBound upper, PlaceBound lower)
   {
      bounds.data.upper = upper;
      bounds.data.lower = lower;
   }

   inline PlaceBound upper() const {
      return bounds.data.upper;
   }
   inline PlaceBound& upper() {
      return bounds.data.upper;
   }
   inline PlaceBound lower() const {
      return bounds.data.lower;
   }
   inline PlaceBound& lower() {
      return bounds.data.lower;
   }

   bool operator==(const PlaceBounds& other) {
      return bounds.comparison == other.bounds.comparison;
   }
};

const PlaceBounds placeSeparator = PlaceBounds { std::numeric_limits<PlaceBound>::max(), std::numeric_limits<PlaceBound>::max() };

static_assert(sizeof(PlaceBounds) == sizeof(void*), "The size of place bounds must match the pointer size for buildPersonPlacesIndex magic to work");
struct PersonPlaceIndex {
   vector<const PlaceBounds*> places;
   const PlaceBounds* dataStart;
};
bool personAtPlace(PersonId p, PlaceBounds bounds, const PersonPlaceIndex& placeIndex);
typedef awfy::unordered_multimap<awfy::StringRef,PlaceId> NamePlaceIndex;
typedef awfy::unordered_map<PlaceId,PlaceBounds> PlaceBoundsIndex;

struct FileIndexes {
   vector<uint8_t*> allocatedBuffers;

   PersonMapper personMapper;
   CommentMapper commentMapper;

   const PersonGraph* personGraph; //q1,q2,q3,q4
   PersonCommentedGraph personCommentedGraph; //q1
   CommentCreatorMap* creatorMap; //q1, but only as an intermediate. Deleted afterwards
   const Birthday* birthdayIndex; //q2
   const HasInterestIndex* hasInterestIndex; //q2,q3
   const TagIndex* tagIndex; //q2,q4
   const PlaceBoundsIndex* placeBoundsIndex; //q3
   const PersonPlaceIndex* personPlaceIndex; //q3
   const NamePlaceIndex* namePlaceIndex; //q3
   TagInForums tagInForumsIndex; //q4
   const HasMemberIndex* hasMemberIndex; //q4
   const InterestStatistics* interestStatistics;

   FileIndexes();

   TaskGroup prepareMappers(Scheduler& scheduler, const string& dataPath, const bool query1, const bool query2, const bool query3, const bool query4);
   void setupIndexTasks(Scheduler& scheduler, ScheduleGraph& taskGraph, const string& dataPath, const boost::unordered_set<awfy::StringRef>& usedTags);
};
