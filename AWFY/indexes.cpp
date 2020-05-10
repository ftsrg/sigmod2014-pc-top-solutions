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

#include <cstdint>
#include <list>
#ifdef __SSE2__
#include <emmintrin.h>
#endif
#include "include/indexers.hpp"
#include "include/alloc.hpp"
#include "include/metrics.hpp"

static const unsigned unroll=32;

TaskGroup scheduleHasInterestIndex(const HasInterestIndex** targetPtr, const string& dataDir, PersonMapper& personMapper)
{
   IdentityMapper<InterestId>* mapper = new IdentityMapper<PersonId>();
   return sortedGroupingIndex<HasInterestIndex, PersonMapper, IdentityMapper<InterestId>>(TaskGraph::HasInterest, targetPtr, dataDir+"person_hasInterest_tag.csv", personMapper, personMapper.count(), *mapper);
}

TaskGroup scheduleTagInForumsIndex(const TagInForumsIndex** targetPtr, unordered_set<ForumId>& forumsOut, const string& dataDir, const unordered_set<InterestId>& usedTags)
{
   IdentityMapper<ForumId>* mapper = new IdentityMapper<ForumId>();
   return unsortedGroupingIndex<TagInForumsIndex, IdentityMapper<InterestId>, IdentityMapper<ForumId>,true /*reverse*/, false, false, true /*keys out*/, true /*filter*/>(TaskGraph::TagInForums, targetPtr, dataDir+"forum_hasTag_tag.csv", *mapper, usedTags.size(), *mapper, forumsOut, usedTags);
}

TaskGroup scheduleHasMemberIndex(const HasMemberIndex** targetPtr, const string& dataDir, PersonMapper& personMapper, const unordered_set<ForumId>& usedForums)
{
   IdentityMapper<ForumId>* mapper = new IdentityMapper<ForumId>();
   unordered_set<ForumId> memberDummy;
   return unsortedGroupingIndex<HasMemberIndex, IdentityMapper<ForumId>, PersonMapper,false, true, true, false /*keys out*/, true /*filter*/>(TaskGraph::HasForum, targetPtr, dataDir+"forum_hasMember_person.csv", *mapper, usedForums.size(), personMapper, memberDummy, usedForums);
}

TagIndex* buildTagIndex(const string& dataDir, const unordered_set<awfy::StringRef>& usedTags)
{
   awfy::AllocatorRef allocator = awfy::Allocator::get();

   io::MmapedFile file(dataDir+"tag.csv", O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);

   const auto numTags = tokenizer.countLines();
   TagIndex* index = new TagIndex(numTags);

   tokenizer.skipAfter('\n'); // Skip header
   do {
      InterestId id=tokenizer.consumeLong('|');
      auto tagStart=tokenizer.getPositionPtr();
      auto tagLength=tokenizer.skipAfterAndCount('|')-1;

      // Copy into string memory area
      const auto strPtr=allocator.alloc<char>(tagLength+1);
      for (unsigned i=0;i<tagLength;++i) // for loop often superior to memcpy
         *(strPtr+i)=*(tagStart+i);
      strPtr[tagLength]=0;

      //If this tag is used in a Q4, add its id to used tags
      awfy::StringRef tagStr(strPtr,tagLength);
      if(usedTags.find(tagStr)!=usedTags.end()) {
         index->usedTags.insert(id);
      }

      index->strToId.insert(tagStr,id);
      index->idToStr.insert(id,move(tagStr));
      tokenizer.skipAfter('\n');
   } while (!tokenizer.finished());

   return index;
}

NamePlaceIndex buildNamePlaceIndex(const string& dataDir)
{
   awfy::AllocatorRef allocator = awfy::Allocator::get();

   io::MmapedFile file(dataDir+"place.csv",O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);
   uint64_t mapSize=tokenizer.countLines()-1;

   NamePlaceIndex mapping;
   mapping.reserve(mapSize);

   tokenizer.skipAfter('\n'); // Skip header
   do {
      const PlaceId placeId=tokenizer.consumeLong('|');
      const char* startPtr=tokenizer.getPositionPtr();
      const auto len=tokenizer.skipAfterAndCount('|')-1;

      // Copy into string memory area
      const auto strPtr=allocator.alloc<char>(len+1);
      for (unsigned i=0;i<len;++i) // for loop often superior to memcpy
         *(strPtr+i)=*(startPtr+i);
      strPtr[len]=0;

      mapping.emplace(make_pair(awfy::StringRef(strPtr,len),placeId));
      tokenizer.skipAfter('\n');
   } while (!tokenizer.finished());

   return move(mapping);
}

#ifdef DEBUG
void checkAcyclic(PlacesTreeElement* element, unordered_set<PlacesTreeElement*>& parents) {
   parents.insert(element);
   const auto& childElements = element->childElements;
   for(auto cIter=childElements.cbegin(); cIter!=childElements.cend(); cIter++) {
      assert(parents.find(*cIter)==parents.end()); //No parent element as child
   }
   for(auto cIter=childElements.cbegin(); cIter!=childElements.cend(); cIter++) {
      checkAcyclic(*cIter, parents);
   }
}
#endif

PlacesTree buildPlacesTree(const string& dataDir)
{
   io::MmapedFile file(dataDir+"place_isPartOf_place.csv",O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);

   boost::unordered_map<PlaceId,PlacesTreeElement> places;
   boost::unordered_set<PlacesTreeElement*> roots;

   tokenizer.skipAfter('\n'); // Skip header
   do {
      auto result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
      //Child
      PlacesTreeElement* cElement;
      {
         auto c=result.first;
         auto cIter=places.find(c);
         if(cIter!=places.end()) {
            cElement=&cIter->second;
            //If the child is a root, remove it
            auto cRootIter=roots.find(cElement);
            if(cRootIter!=roots.end()) {
               roots.erase(cRootIter);
            }
         } else {
            cElement=&(places.insert(make_pair(c,PlacesTreeElement(c))).first->second);
         }
      }
      //Parent
      PlacesTreeElement* pElement;
      {
         auto p=result.second;
         auto pIter=places.find(p);
         if(pIter!=places.end()) {
            pElement=&pIter->second;
         } else {
            pElement=&(places.insert(make_pair(p,PlacesTreeElement(p))).first->second);
            //New parent element creates -> add it as a current root
            roots.insert(pElement);
         }
      }
      //Assemble
      pElement->childElements.push_back(cElement);
   } while (!tokenizer.finished());

   #ifdef DEBUG
   for(auto rIter=roots.begin(); rIter!=roots.end(); rIter++) {
      unordered_set<PlacesTreeElement*> parents;
      checkAcyclic(*rIter, parents);
   }
   #endif

   return PlacesTree(PlacesTreeElement(0, vector<PlacesTreeElement*>(std::make_move_iterator(roots.begin()), std::make_move_iterator(roots.end()))), std::move(places));
}


void assignPlaceBounds(const PlacesTreeElement* place, PlaceBound& maxBound, PlaceBoundsIndex& index)
{
   PlaceBounds placeBounds;
   placeBounds.lower() = maxBound;
   const auto& childElements=place->childElements;
   for(auto cIter=childElements.cbegin(); cIter!=childElements.cend(); cIter++) {
      assignPlaceBounds(*cIter, maxBound, index);
      maxBound++;
   }
   placeBounds.upper()=maxBound;
   index.emplace(place->placeId, move(placeBounds));
}

PlaceBoundsIndex buildPlaceBoundsIndex(const string& dataDir)
{
   const PlacesTree placesTree=buildPlacesTree(dataDir);

   PlaceBoundsIndex boundsIndex;
   //Process all roots
   PlaceBound maxBound=0;
   const auto& childElements=placesTree.root.childElements;
   for(auto cIter=childElements.cbegin(); cIter!=childElements.cend(); cIter++) {
      assignPlaceBounds(*cIter, maxBound, boundsIndex);
      maxBound++;
   }
   return move(boundsIndex);
}

vector<PlaceId> buildOrganizationPlaceIndex(const string& dataDir)
{
   io::MmapedFile file(dataDir+"organisation_isLocatedIn_place.csv", O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);
   const uint64_t vectorSize=tokenizer.countLines()-1;

   vector<PlaceId> organizationPlaces;
   organizationPlaces.reserve(vectorSize);

#ifdef DEBUG
   OrganizationId n=0;
#endif
   tokenizer.skipAfter('\n'); // skip header
   do {
      auto result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
      PlaceId place=result.second;
#ifdef DEBUG
      OrganizationId org=result.first;
      assert(org == n); //Validate assumption that first column is sorted and in steps of 10
      n += 10;
#endif
      organizationPlaces.push_back(place);
   } while (!tokenizer.finished());

   return organizationPlaces;
}

typedef list<PlaceId> PlaceList;
PlaceList*& getPlaceList(PersonId p, PersonPlaceIndex& index) {
   if(p>=index.places.size()) {
      index.places.resize(p+1);
   }
   PlaceList*& personPlaceList = (PlaceList*&)index.places[p];
   if(personPlaceList == nullptr) {
      personPlaceList = new PlaceList();
   }
   return personPlaceList;
}

void readOrganizationsFromFile(const string& path, PersonPlaceIndex& index, const vector<PlaceId>& organizationPlaces, PersonMapper& personMapper, size_t& dataLen) {
   io::MmapedFile file(path,O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);

   tokenizer.skipAfter('\n'); // Skip header
   do {
      auto result=tokenizer.consumeLongLongSingleDelimiter('|');
      PersonId person=personMapper.map(result.first);
      PlaceId place=result.second;
      tokenizer.skipAfter('\n');

      getPlaceList(person,index)->push_front(organizationPlaces.at(place/10));
      dataLen++;
   } while (!tokenizer.finished());
}

PersonPlaceIndex buildPersonPlacesIndex(const string& dataDir, PersonMapper& personMapper, const PlaceBoundsIndex& boundsIndex)
{
   PersonPlaceIndex index;

   size_t dataLen = 0;
   {
      io::MmapedFile file(dataDir+"person_isLocatedIn_place.csv",O_RDONLY);
      madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
      tokenize::Tokenizer tokenizer(file);

      tokenizer.skipAfter('\n'); // Skip header
      do {
         auto result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
         PersonId person=personMapper.map(result.first);
         PlaceId place=result.second;

         getPlaceList(person,index)->push_front(place);
         dataLen += 2; // Place + separator
      } while (!tokenizer.finished());
   }

   const auto organizationPlaces = buildOrganizationPlaceIndex(dataDir);

   readOrganizationsFromFile(dataDir+"person_studyAt_organisation.csv", index, organizationPlaces, personMapper, dataLen);
   readOrganizationsFromFile(dataDir+"person_workAt_organisation.csv", index, organizationPlaces, personMapper, dataLen);

   PlaceBounds* dataPos = new PlaceBounds[dataLen];
   index.dataStart = dataPos;
   for(PersonId p=0; p<index.places.size(); p++) {
      const auto dataStart = dataPos;
      const auto listPtr = getPlaceList(p,index);
      const auto& list = *listPtr;
      for(auto lIter=list.cbegin(); lIter!=list.cend(); lIter++) {
         *dataPos = boundsIndex.at(*lIter);
         dataPos++;
      }
      *dataPos = placeSeparator;
      dataPos++;
      delete listPtr;
      index.places[p] = dataStart;
   }
   return index;
}

bool personAtPlace(PersonId p, PlaceBounds bounds, const PersonPlaceIndex& placeIndex)
{
   const PlaceBounds* personPlace = placeIndex.places[p];
   static_assert(2*sizeof(PersonId) == sizeof(uint64_t), "personAtPlace data type size constraints violated");
   while(*reinterpret_cast<const uint64_t*>(personPlace) != *reinterpret_cast<const uint64_t*>(&placeSeparator)) {
      if(bounds.lower()<=personPlace->lower() && bounds.upper()>=personPlace->upper()) {
         return true;
      }
      personPlace++;
   }
   return false;
}

Birthday* buildPersonBirthdayIndex(const string& dataDir, PersonMapper& personMapper) {
   Birthday* index;
   const auto numPersons=personMapper.count();
   auto ret=posix_memalign(reinterpret_cast<void**>(&index),64,numPersons*sizeof(Birthday));
   if(unlikely(ret!=0)) {
      throw -1;
   }

   io::MmapedFile file(dataDir+"person.csv", O_RDONLY);
   madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
   tokenize::Tokenizer tokenizer(file);

   tokenizer.skipAfter('\n'); // Skip header
   do {
      PersonId id=personMapper.map(tokenizer.consumeLong('|'));
      tokenizer.skipAfter('|'); //firstName
      tokenizer.skipAfter('|'); //lastName
      tokenizer.skipAfter('|'); //gender
      // ID has to fit
      assert(id<numPersons);
      // Add birthday to index
      index[id]=tokenizer.consumeBirthday();
      tokenizer.skipAfter('\n');
   } while (!tokenizer.finished());

   return index;
}

#define MADVISEAFTER 8

template <class CommentMapper>
struct BuildOrSchedulePersonCommentedIndex_ProcessChunk {
   const unsigned i;
   ChunkTokenizer* const chunks;
   uint8_t* const baseCommentedPtr;
   uint8_t* const basePersonPtr;
   CommentCreatorMap* const commentCreators;
   const PersonGraph& personGraph;
   CommentMapper& commentMapper;

   BuildOrSchedulePersonCommentedIndex_ProcessChunk(const unsigned i,ChunkTokenizer* const chunks,uint8_t* const baseCommentedPtr,uint8_t* const basePersonPtr,CommentCreatorMap* const commentCreators,const PersonGraph& personGraph,CommentMapper& commentMapper)
   : i(i), chunks(chunks), baseCommentedPtr(baseCommentedPtr), basePersonPtr(basePersonPtr), commentCreators(commentCreators), personGraph(personGraph), commentMapper(commentMapper)
   { }

   void operator()() {
      tokenize::Tokenizer innerTokenizer=chunks->getTokenizer(i);

      // run madvise after every MADVISEAFTER chunks
      if (i%MADVISEAFTER==0) {
         void* start=innerTokenizer.getPositionPtrVoid();
         madvise(start,MADVISEAFTER*chunks->chunkSize,MADV_WILLNEED|MADV_SEQUENTIAL);
      }

      unsigned parsed=0;
      uint64_t cids[unroll];
      uint64_t pids[unroll];
      
      parse:
      for (;parsed<unroll;) {
         if (unlikely(innerTokenizer.finished())) break;
         ++parsed;
         auto result=innerTokenizer.consumeLongLongDistinctDelimiter('|','\n');
         cids[parsed-1]=result.first;
         pids[parsed-1]=result.second;
      }
      for (unsigned i=0;i<parsed;++i) {
         auto commentId=commentMapper.map(cids[i]);
         auto parentId=commentMapper.map(pids[i]);
         // Map comments to person and check if they are connected
         auto commentPersonId=commentCreators->retrieve(commentId);
         auto parentPersonId=commentCreators->retrieve(parentId);
         // Skip self references as these edges will be always in the graph
         if(commentPersonId==parentPersonId) {
            continue;
         }
         auto neighbours = personGraph.retrieve(parentPersonId);
         if(neighbours == nullptr) {
            continue;
         }
         auto neighbourOffset = neighbours->find(commentPersonId);
         if (neighbourOffset == nullptr) {
            continue;
         }

         // Increment counter for interaction of child with parent (we use exact same layout as person graph thus we use same offset)
         auto commentedOffset=reinterpret_cast<const uint8_t*>(neighbourOffset)-basePersonPtr;
         auto commentedPtr=baseCommentedPtr+commentedOffset;
         __sync_fetch_and_add(commentedPtr,1);
      }
      if (likely(!innerTokenizer.finished())) {
         parsed=0;
         goto parse;
      }
   }
};

struct BuildOrSchedulePersonCommentedIndex_Join {
   io::MmapedFile* const file;
   FileIndexes* const indexes;
   uint8_t* const baseCommentedPtr;
   ChunkTokenizer* const chunks;

   BuildOrSchedulePersonCommentedIndex_Join(io::MmapedFile* const file,FileIndexes* const indexes,uint8_t* const baseCommentedPtr,ChunkTokenizer* const chunks)
   : file(file), indexes(indexes), baseCommentedPtr(baseCommentedPtr), chunks(chunks)
   { }

   void operator()() {
      delete chunks;
      delete file;
      indexes->personCommentedGraph=baseCommentedPtr;
   }
};

template <class CommentMapper>
TaskGroup buildOrSchedulePersonCommentedIndex(FileIndexes* indexes, const string& path, CommentCreatorMap* commentCreators, const PersonGraph& personGraph, CommentMapper& commentMapper) {

   io::MmapedFile* file=new io::MmapedFile(path,O_RDONLY);
   madvise(reinterpret_cast<void*>(file->mapping),file->size,MADV_WILLNEED);
   assert(&personGraph);

   // Create copy of PersonGraph where we store the comment count
   uint8_t* basePersonPtr=reinterpret_cast<uint8_t*>(personGraph.buffer.data);
   auto dataSize=personGraph.buffer.size;
   uint8_t* baseCommentedPtr;
   auto ret=posix_memalign(reinterpret_cast<void**>(&baseCommentedPtr),64,dataSize);
   if(unlikely(ret!=0)) {
      throw -1;
   }
   memset(baseCommentedPtr,0,dataSize);

   // Split file into chunks
   tokenize::Tokenizer tokenizer(*file);
   tokenizer.skipAfter('\n'); //Skip header
   size_t maxChunks=32768; // essentially unlimited
   size_t chunkSize=1<<21;
   ChunkTokenizer* chunks = new ChunkTokenizer(tokenizer, chunkSize, maxChunks);

   TaskGroup readTasks;
   size_t numChunks=chunks->getNumChunks();
   for (unsigned i=0;i<numChunks;++i) {
      readTasks.schedule(LambdaRunner::createLambdaTask(BuildOrSchedulePersonCommentedIndex_ProcessChunk<CommentMapper>(i,chunks,baseCommentedPtr,basePersonPtr,commentCreators,personGraph,commentMapper),TaskGraph::PersonCommented));
   }
   readTasks.join(LambdaRunner::createLambdaTask(BuildOrSchedulePersonCommentedIndex_Join(file, indexes,baseCommentedPtr,chunks),TaskGraph::PersonCommented));

   return readTasks;
}

struct BuildPersonCommentedIndex_Streaming_ProcessChunk {
   static const uint32_t creatorLookupTableSize = 1<<7; // 128
   static const uint32_t creatorLookupTableMask = 2*creatorLookupTableSize-1;
   static const size_t commentScanChunkSize = 12*1024*1024; //Interval used for the initial scan of the comment->creator file

   const unsigned i;
   ChunkTokenizer* replyOfCommentChunks;
   uint8_t* basePersonPtr;
   uint8_t* baseCommentedPtr;
   io::MmapedFile* commentCreatorFile;
   const PersonGraph& personGraph;
   PersonMapper& personMapper;
   const vector<pair<CommentId, const char*>>* commentPositions;
   CommentMapper& commentMapper;
   const string commentCreatorPath;

   BuildPersonCommentedIndex_Streaming_ProcessChunk(const unsigned i, ChunkTokenizer* replyOfCommentChunks, uint8_t* basePersonPtr, uint8_t* baseCommentedPtr, io::MmapedFile* commentCreatorFile, const PersonGraph& personGraph, PersonMapper& personMapper, const vector<pair<CommentId, const char*>>* commentPositions, CommentMapper& commentMapper, const string commentCreatorPath)
      : i(i), replyOfCommentChunks(replyOfCommentChunks), basePersonPtr(basePersonPtr), baseCommentedPtr(baseCommentedPtr), commentCreatorFile(commentCreatorFile), personGraph(personGraph), personMapper(personMapper), commentPositions(commentPositions), commentMapper(commentMapper), commentCreatorPath(commentCreatorPath)
   { }

   void fallback(tokenize::Tokenizer& replyOfFileTokenizer, uint64_t replyCommentId, uint64_t baseCommentId) {
      cout<<"FALLBACK"<<endl;
      //Index comment has creator file so that tasks can find merge start position easily
      io::MmapedFile* commentCreatorFile=new io::MmapedFile(commentCreatorPath,O_RDONLY);

      tokenize::Tokenizer commentCreatorTokenizer(*commentCreatorFile);
      commentCreatorTokenizer.skipAfter('\n'); // skip header
      const size_t num = commentCreatorTokenizer.countLines();

      CommentCreatorMap commentCreatorIndex(num);
      while(!commentCreatorTokenizer.finished()) {
         auto result=commentCreatorTokenizer.consumeLongLongDistinctDelimiter('|','\n');
         auto commentId=commentMapper.map(result.first);
         auto personId=personMapper.map(result.second);

         commentCreatorIndex.insert(commentId, personId);
      }

      const auto neighbours = personGraph.retrieve(commentCreatorIndex.retrieve(baseCommentId));
      if(neighbours != nullptr) {
         const auto neighbourOffset = neighbours->find(commentCreatorIndex.retrieve(replyCommentId));
         if (neighbourOffset != nullptr) {
            // Increment counter for interaction of child with parent (we use exact same layout as person graph thus we use same offset)
            const auto commentedOffset=reinterpret_cast<const uint8_t*>(neighbourOffset)-basePersonPtr;
            const auto commentedPtr=baseCommentedPtr+commentedOffset;
            __sync_fetch_and_add(commentedPtr,1);
         }
      }

      while(!replyOfFileTokenizer.finished()) {
         const auto replyOfEntry = replyOfFileTokenizer.consumeLongLongDistinctDelimiter('|','\n');
         assert(replyOfEntry.first > replyOfEntry.second); //We reply to an existing comment

         const uint64_t replyCommentId = commentMapper.map(replyOfEntry.first);
         const uint64_t baseCommentId = commentMapper.map(replyOfEntry.second);

         const auto neighbours = personGraph.retrieve(commentCreatorIndex.retrieve(baseCommentId));
         if(neighbours == nullptr) {
            continue;
         }
         const auto neighbourOffset = neighbours->find(commentCreatorIndex.retrieve(replyCommentId));
         if (neighbourOffset == nullptr) {
            continue;
         }

         // Increment counter for interaction of child with parent (we use exact same layout as person graph thus we use same offset)
         const auto commentedOffset=reinterpret_cast<const uint8_t*>(neighbourOffset)-basePersonPtr;
         const auto commentedPtr=baseCommentedPtr+commentedOffset;
         __sync_fetch_and_add(commentedPtr,1);
      }

      delete commentCreatorFile;
   }

   void operator()() {
      tokenize::Tokenizer replyOfFileTokenizer=replyOfCommentChunks->getTokenizer(i);
      const auto chunkStart = replyOfFileTokenizer.getPositionPtr();
      const auto chunkSize = replyOfCommentChunks->chunkSize;
      madvise(replyOfFileTokenizer.getPositionPtrVoid(),chunkSize,MADV_WILLNEED|MADV_SEQUENTIAL);

      //Find min comment id in this chunk
      CommentId minChunkCommentId = numeric_limits<CommentId>::max();
      for(uint32_t a=0; a<creatorLookupTableSize; a++) {
         replyOfFileTokenizer.skipAfter('|');
         minChunkCommentId = min(minChunkCommentId, commentMapper.map(replyOfFileTokenizer.consumeLong('\n')));
      }
      //Reset comment reply of tokenizer
      replyOfFileTokenizer.setPositionPtr(chunkStart);

      //Find min comment id in comment has creator file
      auto commentPositionsIter = commentPositions->cbegin()+1;
      while(commentPositionsIter->first<=minChunkCommentId) {
         ++commentPositionsIter;
      }
      --commentPositionsIter;

      tokenize::Tokenizer commentCreatorTokenizer(*commentCreatorFile,commentPositionsIter->second);
      madvise(commentCreatorTokenizer.getPositionPtrVoid(),commentScanChunkSize,MADV_WILLNEED|MADV_SEQUENTIAL);
      const char* lastCreatorStart;
      CommentId hasCreatorCommentId;
      do {
         lastCreatorStart = commentCreatorTokenizer.getPositionPtr();
         hasCreatorCommentId = commentMapper.map(commentCreatorTokenizer.consumeLong('|'));
         commentCreatorTokenizer.skipAfter('\n');
      } while(hasCreatorCommentId<minChunkCommentId);
      commentCreatorTokenizer.setPositionPtr(lastCreatorStart);

      //Prepare lookup table
      uint32_t creatorLookupTable[2*creatorLookupTableSize];
      uint32_t lastLookupTableComment = hasCreatorCommentId + fillLookupTable(commentCreatorTokenizer, creatorLookupTable, personMapper, commentMapper) - 1;

      //Read comments
      while(!replyOfFileTokenizer.finished()) {
         //Get comment
         const auto replyOfEntry = replyOfFileTokenizer.consumeLongLongDistinctDelimiter('|','\n');
         assert(replyOfEntry.first > replyOfEntry.second); //We reply to an existing comment

         const uint64_t replyCommentId = commentMapper.map(replyOfEntry.first);
         const uint64_t baseCommentId = commentMapper.map(replyOfEntry.second);

         //Check that the streaming assumptions still hold. Fallback to slower logic if not
         if(unlikely(replyCommentId-baseCommentId > creatorLookupTableSize)) {
            //fallback logic
            fallback(replyOfFileTokenizer, replyCommentId,baseCommentId);
            return;
         }

         //Check if we have the entries still in the lookup. If not, fetch further entries
         if(replyCommentId > lastLookupTableComment) {
            lastLookupTableComment += fillLookupTable(commentCreatorTokenizer, creatorLookupTable, personMapper, commentMapper);
         }

         //Get reply creator and base comment creator
         const auto replyCreatorId = creatorLookupTable[replyCommentId & creatorLookupTableMask];
         const auto baseCreatorId = creatorLookupTable[baseCommentId & creatorLookupTableMask];

         const auto neighbours = personGraph.retrieve(baseCreatorId);
         if (unlikely(neighbours==nullptr)) {
            continue;
         }
         const auto neighbourOffset = neighbours->find(replyCreatorId);
         if (neighbourOffset==nullptr) {
            continue;
         }

         // Increment counter for interaction of child with parent (we use exact same layout as person graph thus we use same offset)
         const auto commentedOffset=reinterpret_cast<const uint8_t*>(neighbourOffset)-basePersonPtr;
         const auto commentedPtr=baseCommentedPtr+commentedOffset;
         __sync_fetch_and_add(commentedPtr,1);
      }
   }

   uint32_t fillLookupTable(tokenize::Tokenizer& commentCreatorTokenizer, uint32_t* creatorLookupTable, PersonMapper& personMapper, CommentMapper& commentMapper) {
      unsigned parsed=0;
      uint64_t cids[creatorLookupTableSize];
      uint64_t pids[creatorLookupTableSize];

      for (;parsed<creatorLookupTableSize;) {
         if (unlikely(commentCreatorTokenizer.finished())) break;
         ++parsed;
         auto result=commentCreatorTokenizer.consumeLongLongDistinctDelimiter('|','\n');
         cids[parsed-1]=result.first;
         pids[parsed-1]=result.second;
      }
      for (unsigned i=0;i<parsed;++i) {
         const auto commentId=commentMapper.map(cids[i]);
         creatorLookupTable[commentId&creatorLookupTableMask]=personMapper.map(pids[i]);
      }
      return parsed;
   }
};

struct BuildPersonCommentedIndex_Streaming_Join {
   io::MmapedFile* commentCreatorFile;
   io::MmapedFile* replyOfCommentFile;
   ChunkTokenizer* replyOfCommentChunks;
   const vector<pair<CommentId, const char*>>* commentPositions;

   BuildPersonCommentedIndex_Streaming_Join(io::MmapedFile* commentCreatorFile, io::MmapedFile* replyOfCommentFile, ChunkTokenizer* replyOfCommentChunks, const vector<pair<CommentId, const char*>>* commentPositions)
   : commentCreatorFile(commentCreatorFile), replyOfCommentFile(replyOfCommentFile), replyOfCommentChunks(replyOfCommentChunks), commentPositions(commentPositions)
   { }

   void operator()() {
      delete commentPositions;
      delete replyOfCommentChunks;
      delete replyOfCommentFile;
      delete commentCreatorFile;
   }
};

TaskGroup schedulePersonCommentedIndex_Streaming(FileIndexes* indexes, io::MmapedFile* commentCreatorFile, const string& commentCreatorPath, const string& replyOfCommentPath, const PersonGraph& personGraph, PersonMapper& personMapper, const vector<pair<CommentId, const char*>>* commentPositions, CommentMapper& commentMapper) {

   //"Copy" person graph
   uint8_t* basePersonPtr=reinterpret_cast<uint8_t*>(personGraph.buffer.data);
   const auto dataSize=personGraph.buffer.size;
   uint8_t* baseCommentedPtr;
   auto ret=posix_memalign(reinterpret_cast<void**>(&baseCommentedPtr),64,dataSize);
   if(unlikely(ret!=0)) {
      throw -1;
   }
   memset(baseCommentedPtr,0,dataSize);

   // Load reply file and split into chunks
   io::MmapedFile* replyOfCommentFile=new io::MmapedFile(replyOfCommentPath,O_RDONLY);
   tokenize::Tokenizer replyOfCommentTokenizer(*replyOfCommentFile);
   replyOfCommentTokenizer.skipAfter('\n'); //Skip header

   size_t maxChunks=32;
   size_t chunkSize=1<<24; //64Mb
   ChunkTokenizer* replyOfCommentChunks = new ChunkTokenizer(replyOfCommentTokenizer, chunkSize, maxChunks);

   TaskGroup readTasks;
   const auto numChunks=replyOfCommentChunks->getNumChunks();
   for (unsigned i=0;i<numChunks;++i) {
      readTasks.schedule(LambdaRunner::createLambdaTask(BuildPersonCommentedIndex_Streaming_ProcessChunk(i,replyOfCommentChunks,basePersonPtr,baseCommentedPtr,commentCreatorFile,personGraph,personMapper, commentPositions,commentMapper, commentCreatorPath),TaskGraph::PersonCommented));
   }
   readTasks.join(LambdaRunner::createLambdaTask(BuildPersonCommentedIndex_Streaming_Join(commentCreatorFile, replyOfCommentFile, replyOfCommentChunks, commentPositions),TaskGraph::PersonCommented));

   indexes->personCommentedGraph = baseCommentedPtr;
   return readTasks;   
}



template<class Builder>
Task builderTask(Builder* builder, TaskGraph::Node node) {
   return Task((void (*)(void*)) &(Builder::build), builder, node);
}

struct BirthdayBuilder {
   const string& dataPath;
   FileIndexes* indexes;

   BirthdayBuilder(const string& dataPath, FileIndexes* indexes) : dataPath(dataPath), indexes(indexes) {
   }

   static void* build(BirthdayBuilder* builder) {
      metrics::BlockStats<>::LogSensor sensor("birthday");
      builder->indexes->birthdayIndex=buildPersonBirthdayIndex(builder->dataPath,builder->indexes->personMapper);
      delete builder;
      return nullptr;
   }
};

struct PersonPlaceBuilder {
   const string& dataPath;
   FileIndexes* indexes;

   PersonPlaceBuilder(const string& dataPath, FileIndexes* indexes) : dataPath(dataPath), indexes(indexes) {
   }

   static void* build(PersonPlaceBuilder* builder) {
      {
         metrics::BlockStats<>::LogSensor sensor("placeBounds");
         builder->indexes->placeBoundsIndex=new PlaceBoundsIndex(buildPlaceBoundsIndex(builder->dataPath));
      }
      {
         metrics::BlockStats<>::LogSensor sensor("personPlace");
         builder->indexes->personPlaceIndex=new PersonPlaceIndex(buildPersonPlacesIndex(builder->dataPath, builder->indexes->personMapper, *(builder->indexes->placeBoundsIndex)));
      }
      delete builder;
      return nullptr;
   }
};

struct TagBuilder {
   const string& dataPath;
   FileIndexes* indexes;
   const unordered_set<awfy::StringRef>& usedTags;

   TagBuilder(const string& dataPath, FileIndexes* indexes, const unordered_set<awfy::StringRef>& usedTags) : dataPath(dataPath), indexes(indexes), usedTags(usedTags) {
   }

   static void* build(TagBuilder* builder) {
      metrics::BlockStats<>::LogSensor sensor("tag");
      builder->indexes->tagIndex=buildTagIndex(builder->dataPath, builder->usedTags);
      delete builder;
      return nullptr;
   }
};

struct NamePlaceBuilder {
   const string& dataPath;
   FileIndexes* indexes;

   NamePlaceBuilder(const string& dataPath, FileIndexes* indexes) : dataPath(dataPath), indexes(indexes) {
   }

   static void* build(NamePlaceBuilder* builder) {
      metrics::BlockStats<>::LogSensor sensor("namePlaceIndex");
      builder->indexes->namePlaceIndex=new NamePlaceIndex(buildNamePlaceIndex(builder->dataPath));
      delete builder;
      return nullptr;
   }
};

struct UpdateTask {
   ScheduleGraph& taskGraph;
   const TaskGraph::Node task;
   UpdateTask(ScheduleGraph& taskGraph, TaskGraph::Node task)
      : taskGraph(taskGraph), task(task)
   { }
   void operator()() {
      taskGraph.updateTask(task, -1);
   }
};

struct HasForumBuilder {
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
   const string& dataPath;
   FileIndexes* indexes;

   HasForumBuilder(ScheduleGraph& taskGraph, Scheduler& scheduler, const string& dataPath, FileIndexes* indexes) : 
         taskGraph(taskGraph), scheduler(scheduler), dataPath(dataPath), indexes(indexes) {
   }

   static void* build(HasForumBuilder* builder) {
      

      auto tasks=scheduleHasMemberIndex(&(builder->indexes->hasMemberIndex), builder->dataPath, builder->indexes->personMapper, builder->indexes->tagInForumsIndex.forums);
      ScheduleGraph& taskGraph=builder->taskGraph;
      tasks.join(LambdaRunner::createLambdaTask(UpdateTask(taskGraph, TaskGraph::HasForum),TaskGraph::HasForum));

      // Only allow to continue after join has finished
      builder->taskGraph.updateTask(TaskGraph::HasForum, 1);
      builder->scheduler.schedule(tasks.close(), Priorities::CRITICAL);
      delete builder;
      return nullptr;
   }
};

struct TagInForumsBuilder {
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
   const string& dataPath;
   FileIndexes* indexes;

   TagInForumsBuilder(ScheduleGraph& taskGraph, Scheduler& scheduler, const string& dataPath, FileIndexes* indexes) : 
         taskGraph(taskGraph), scheduler(scheduler), dataPath(dataPath), indexes(indexes) {
   }

   static void* build(TagInForumsBuilder* builder) {
      assert(builder->indexes->tagIndex!=nullptr);
      auto tasks=scheduleTagInForumsIndex(&(builder->indexes->tagInForumsIndex.index), builder->indexes->tagInForumsIndex.forums, builder->dataPath, builder->indexes->tagIndex->usedTags);
      ScheduleGraph& taskGraph=builder->taskGraph;
      tasks.join(LambdaRunner::createLambdaTask(UpdateTask(taskGraph,TaskGraph::TagInForums),TaskGraph::TagInForums));

      // Only allow to continue after join has finished
      builder->taskGraph.updateTask(TaskGraph::TagInForums, 1);
      builder->scheduler.schedule(tasks.close(), Priorities::CRITICAL);
      delete builder;
      return nullptr;
   }
};

struct HasInterestBuilder {
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
   const string& dataPath;
   FileIndexes* indexes;

   HasInterestBuilder(ScheduleGraph& taskGraph, Scheduler& scheduler, const string& dataPath, FileIndexes* indexes) : 
         taskGraph(taskGraph), scheduler(scheduler), dataPath(dataPath), indexes(indexes) {
   }

   static void* build(HasInterestBuilder* builder) {

      auto tasks=scheduleHasInterestIndex(&(builder->indexes->hasInterestIndex), builder->dataPath, builder->indexes->personMapper);
      ScheduleGraph& taskGraph=builder->taskGraph;
      tasks.join(LambdaRunner::createLambdaTask(UpdateTask(taskGraph,TaskGraph::HasInterest),TaskGraph::HasInterest));

      // Only allow to continue after join has finished
      builder->taskGraph.updateTask(TaskGraph::HasInterest, 1);
      builder->scheduler.schedule(tasks.close(), Priorities::CRITICAL);
      delete builder;
      return nullptr;
   }
};

struct InterestStatisticsBuilder {
   struct InterestComparer {
      bool operator()(const InterestStat& a, const InterestStat& b) {
         return a.numPersons>b.numPersons;
      }
   };

   FileIndexes* indexes;

   InterestStatisticsBuilder(FileIndexes* indexes) : indexes(indexes) {
   }

   static void* build(InterestStatisticsBuilder* builder) {
      const HasInterestIndex& interestIndex=*builder->indexes->hasInterestIndex;
      const Birthday* birthdayIndex=builder->indexes->birthdayIndex;
      const PersonMapper& personMapper=builder->indexes->personMapper;

      std::unordered_map<InterestId, InterestStat> interestStats;

      for(PersonId person=0; person<personMapper.count(); person++) {
         const auto interests = interestIndex.retrieve(person);
         if(interests==nullptr) { continue; }
         auto bounds = interests->bounds();
         while(bounds.first!=bounds.second) {
            const InterestId interest=*bounds.first;
            auto entry = interestStats.find(interest);
            if(entry==interestStats.end()) {
               entry = interestStats.insert(std::make_pair(interest, InterestStat(interest))).first;
            }

            entry->second.numPersons++;
            if(birthdayIndex[person] >= entry->second.maxBirthday) {
               entry->second.maxBirthday=birthdayIndex[person];
            }

            bounds.first++;
         }
      }

      std::vector<InterestStat>* interestStatIx = new std::vector<InterestStat>();
      interestStatIx->reserve(interestStats.size());
      for ( auto it = interestStats.begin(); it != interestStats.end(); ++it ) {
         interestStatIx->push_back(move(it->second));
      }
      sort(interestStatIx->begin(), interestStatIx->end(), InterestComparer());

      builder->indexes->interestStatistics = interestStatIx;

      delete builder;
      return nullptr;
   }
};

struct PersonGraphBuilder {
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
   const string& dataPath;
   FileIndexes* indexes;

   PersonGraphBuilder(ScheduleGraph& taskGraph, Scheduler& scheduler, const string& dataPath, FileIndexes* indexes) : 
         taskGraph(taskGraph), scheduler(scheduler), dataPath(dataPath), indexes(indexes) {
   }

   static void* build(PersonGraphBuilder* builder) {
      auto tasks=schedulePersonGraph(&(builder->indexes->personGraph), builder->dataPath, builder->indexes->personMapper);
      ScheduleGraph& taskGraph=builder->taskGraph;
      tasks.join(LambdaRunner::createLambdaTask(UpdateTask(taskGraph,TaskGraph::PersonGraph),TaskGraph::PersonGraph));

      // Only allow to continue after join has finished
      builder->taskGraph.updateTask(TaskGraph::PersonGraph, 1);
      builder->scheduler.schedule(tasks.close(), Priorities::CRITICAL);
      delete builder;
      return nullptr;
   }
};


struct CommentCreatorMapBuilder {
   ScheduleGraph& taskGraph;
   Scheduler& scheduler;
   const string& dataPath;
   FileIndexes* indexes;

   CommentCreatorMapBuilder(ScheduleGraph& taskGraph, Scheduler& scheduler, const string& dataPath, FileIndexes* indexes) : 
         taskGraph(taskGraph), scheduler(scheduler), dataPath(dataPath), indexes(indexes) {
   }

   struct UpdateAndClean {
      ScheduleGraph& taskGraph;
      UpdateAndClean(ScheduleGraph& taskGraph)
         : taskGraph(taskGraph)
      { }

      void operator()() {
         taskGraph.updateTask(TaskGraph::CommentCreatorMap, -1);
      }
   };

   static void* build(CommentCreatorMapBuilder* builder) {

      //Index comment has creator file so that tasks can find merge start position easily
      io::MmapedFile* commentCreatorFile=new io::MmapedFile(builder->dataPath+CSVFiles::CommentCreatorGraph,O_RDONLY);
      madvise(reinterpret_cast<void*>(commentCreatorFile->mapping),commentCreatorFile->size,MADV_WILLNEED|MADV_RANDOM);

      auto& commentMapper = builder->indexes->commentMapper;
      vector<pair<CommentId, const char*>>* commentPositions = new vector<pair<CommentId, const char*>>();
      size_t curScanPos=0;
      do {
         tokenize::Tokenizer commentCreatorTokenizer(*commentCreatorFile, curScanPos);
         commentCreatorTokenizer.skipAfter('\n'); //Skip to next line
         if(unlikely(commentCreatorTokenizer.finished())) {
            break;
         }
         const char* pos = commentCreatorTokenizer.getPositionPtr();
         const CommentId comment = commentMapper.map(commentCreatorTokenizer.consumeLong('|'));
         commentPositions->push_back(make_pair(comment, pos));
         curScanPos += BuildPersonCommentedIndex_Streaming_ProcessChunk::commentScanChunkSize;
      } while(curScanPos < commentCreatorFile->size);
      commentPositions->push_back(make_pair<CommentId,const char*>(numeric_limits<CommentId>::max(), nullptr)); //Invalid entry at the end -> makes later scan over data easier

      auto tasks = schedulePersonCommentedIndex_Streaming(builder->indexes, commentCreatorFile, builder->dataPath+CSVFiles::CommentCreatorGraph, builder->dataPath+CSVFiles::CommentsGraph, *(builder->indexes->personGraph), builder->indexes->personMapper, commentPositions, commentMapper);
      ScheduleGraph& taskGraph=builder->taskGraph;
      tasks.join(LambdaRunner::createLambdaTask(UpdateAndClean(taskGraph),TaskGraph::CommentCreatorMap));

      // Only allow to continue after join has finished
      builder->taskGraph.updateTask(TaskGraph::CommentCreatorMap, 1);
      builder->scheduler.schedule(tasks.close(), Priorities::URGENT);
      return nullptr;
   }
};

struct PersonMappingBuilder {
   const string& dataPath;
   FileIndexes* indexes;

   PersonMappingBuilder(const string& dataPath, FileIndexes* indexes) : dataPath(dataPath), indexes(indexes) {
   }

   static void* build(PersonMappingBuilder* builder) {
      metrics::BlockStats<>::LogSensor sensor("personMapping");

      io::MmapedFile file(builder->dataPath+"person.csv",O_RDONLY);
      madvise(reinterpret_cast<void*>(file.mapping),file.size,MADV_SEQUENTIAL|MADV_WILLNEED);
      tokenize::Tokenizer tokenizer(file);

      size_t numPersons = tokenizer.countLines()-1;
      builder->indexes->personMapper = PersonMapper(numPersons);

      tokenizer.skipAfter('\n'); // Skip header
      do {
         builder->indexes->personMapper.map(tokenizer.consumeLong('|'));
         tokenizer.skipAfter('\n');
      } while (!tokenizer.finished());

      #ifdef DEBUG
      builder->indexes->personMapper.closed=true;
      #endif

      delete builder;
      return nullptr;
   }
};

FileIndexes::FileIndexes() : personGraph(nullptr), personCommentedGraph(nullptr), birthdayIndex(nullptr),
   hasInterestIndex(nullptr), tagIndex(nullptr), placeBoundsIndex(nullptr), personPlaceIndex(nullptr),
   namePlaceIndex(nullptr), hasMemberIndex(nullptr) {

}
void FileIndexes::setupIndexTasks(Scheduler& scheduler, ScheduleGraph& taskGraph, const string& dataPath, const unordered_set<awfy::StringRef>& usedTags) {
   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::PersonMapping,
      builderTask(new PersonMappingBuilder(dataPath, this), TaskGraph::PersonMapping));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::PersonGraph,
      builderTask(new PersonGraphBuilder(taskGraph, scheduler, dataPath, this), TaskGraph::PersonGraph));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::CommentCreatorMap,
      builderTask(new CommentCreatorMapBuilder(taskGraph, scheduler, dataPath, this), TaskGraph::CommentCreatorMap));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::HasInterest,
      builderTask(new HasInterestBuilder(taskGraph, scheduler, dataPath, this), TaskGraph::HasInterest));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::Birthday, 
      builderTask(new BirthdayBuilder(dataPath, this), TaskGraph::Birthday));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::PersonPlace, 
      builderTask(new PersonPlaceBuilder(dataPath, this), TaskGraph::PersonPlace));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::HasForum, 
      builderTask(new HasForumBuilder(taskGraph, scheduler, dataPath, this), TaskGraph::HasForum));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::Tag,
      builderTask(new TagBuilder(dataPath, this, usedTags), TaskGraph::Tag));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::NamePlace, 
      builderTask(new NamePlaceBuilder(dataPath, this), TaskGraph::NamePlace));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::TagInForums,
      builderTask(new TagInForumsBuilder(taskGraph, scheduler, dataPath, this),  TaskGraph::TagInForums));

   taskGraph.setTaskFn(Priorities::CRITICAL, TaskGraph::InterestStatistics,
      builderTask(new InterestStatisticsBuilder(this),  TaskGraph::InterestStatistics));

   // Add indexing dependencies
   taskGraph.addEdge(TaskGraph::Initialize, TaskGraph::PersonMapping);
   taskGraph.addEdge(TaskGraph::PersonMapping, TaskGraph::PersonGraph);
   taskGraph.addEdge(TaskGraph::PersonMapping, TaskGraph::HasInterest);
   taskGraph.addEdge(TaskGraph::PersonMapping, TaskGraph::Birthday);
   taskGraph.addEdge(TaskGraph::PersonMapping, TaskGraph::PersonPlace);
   taskGraph.addEdge(TaskGraph::PersonGraph, TaskGraph::CommentCreatorMap);
   taskGraph.addEdge(TaskGraph::IndexQ2orQ3, TaskGraph::HasInterest);
   taskGraph.addEdge(TaskGraph::IndexQ2, TaskGraph::Birthday);
   taskGraph.addEdge(TaskGraph::IndexQ3, TaskGraph::PersonPlace);
   taskGraph.addEdge(TaskGraph::IndexQ4, TaskGraph::HasForum);
   taskGraph.addEdge(TaskGraph::IndexQ2orQ4, TaskGraph::Tag);
   taskGraph.addEdge(TaskGraph::IndexQ3, TaskGraph::NamePlace);
   taskGraph.addEdge(TaskGraph::QueryLoading, TaskGraph::Tag);
   taskGraph.addEdge(TaskGraph::Tag, TaskGraph::TagInForums);
   taskGraph.addEdge(TaskGraph::TagInForums, TaskGraph::HasForum);
   taskGraph.addEdge(TaskGraph::IndexQ4, TaskGraph::TagInForums);
   taskGraph.addEdge(TaskGraph::HasInterest, TaskGraph::InterestStatistics);
   taskGraph.addEdge(TaskGraph::Birthday, TaskGraph::InterestStatistics);

   // Add query dependencies
   taskGraph.addEdge(TaskGraph::PersonGraph, TaskGraph::Query4);
   taskGraph.addEdge(TaskGraph::PersonGraph, TaskGraph::Query1);
   taskGraph.addEdge(TaskGraph::CommentCreatorMap, TaskGraph::Query1);
   taskGraph.addEdge(TaskGraph::PersonGraph, TaskGraph::Query2);
   taskGraph.addEdge(TaskGraph::HasInterest, TaskGraph::Query2);
   taskGraph.addEdge(TaskGraph::Birthday, TaskGraph::Query2);
   taskGraph.addEdge(TaskGraph::InterestStatistics, TaskGraph::Query2);
   taskGraph.addEdge(TaskGraph::Tag, TaskGraph::Query2);
   taskGraph.addEdge(TaskGraph::PersonGraph, TaskGraph::Query3);
   taskGraph.addEdge(TaskGraph::HasInterest, TaskGraph::Query3);
   taskGraph.addEdge(TaskGraph::PersonPlace, TaskGraph::Query3);
   taskGraph.addEdge(TaskGraph::NamePlace, TaskGraph::Query3);
   taskGraph.addEdge(TaskGraph::HasForum, TaskGraph::Query4);
   taskGraph.addEdge(TaskGraph::Tag, TaskGraph::Query4);
   taskGraph.addEdge(TaskGraph::TagInForums, TaskGraph::Query4);
}
