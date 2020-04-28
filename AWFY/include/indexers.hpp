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

#include "indexes.hpp"
#include "metrics.hpp"
#include "schedulegraph.hpp"
#include <thread>
#include <vector>

namespace CSVFiles {
   const string CommentCreatorGraph = "comment_hasCreator_person.csv";
   const string CommentsGraph = "comment_replyOf_comment.csv";
   const string PersonGraph = "person_knows_person.csv";   
}

class ChunkTokenizer {

public:
   const char* startPtr;
   const char* limit;
   size_t numChunks;
   size_t chunkSize;

   ChunkTokenizer(tokenize::Tokenizer& tokenizer, size_t desiredChunkSize, size_t maxChunks) : 
         startPtr(tokenizer.getPositionPtr()), limit(tokenizer.limit), chunkSize(desiredChunkSize) {
      const size_t length = limit-startPtr;

      numChunks = length/chunkSize+1;
      if(numChunks > maxChunks) {
         chunkSize = length/maxChunks;
         numChunks = maxChunks;
      }
   }

   size_t getNumChunks() {
      return numChunks;
   }

   tokenize::Tokenizer getTokenizer(size_t chunk) {
      const char* chunkIter=startPtr+chunk*chunkSize;
      const char* chunkLimit;
      if(chunk==numChunks-1) {
         chunkLimit = limit;
      } else {
         chunkLimit = chunkIter+chunkSize;
         if(chunkLimit>0 && *(chunkLimit-1)=='\n') {
            chunkLimit++;
         }
      }
      tokenize::Tokenizer chunkTokenizer(chunkIter, chunkLimit-chunkIter);
      if (chunk>0) {
         chunkTokenizer.skipAfter('\n');
      } else if (chunk!=numChunks-1) {
         tokenize::Tokenizer extendTokenizer(chunkLimit,(limit-chunkIter));
         extendTokenizer.skipAfter('\n');
         chunkTokenizer.limit=extendTokenizer.getPositionPtr()-1;
      }
      return chunkTokenizer;
   }
};

template<class TargetIndex, class KeyMapper, class ValueMapper>
struct UniqueMappingIndex_ReadChunk {
   unsigned i;
   ChunkTokenizer* chunks;
   KeyMapper& keyMapper;
   ValueMapper& valueMapper;
   TargetIndex* index;
   UniqueMappingIndex_ReadChunk(unsigned i, ChunkTokenizer* chunks, KeyMapper& keyMapper, ValueMapper& valueMapper, TargetIndex* index)
      : i(i), chunks(chunks), keyMapper(keyMapper), valueMapper(valueMapper), index(index)
   { }

   void operator()() {
      typedef typename TargetIndex::Id Id;
      
      tokenize::Tokenizer innerTokenizer=chunks->getTokenizer(i);
      while(!innerTokenizer.finished()) {
        auto res=innerTokenizer.consumeLongLongDistinctDelimiter('|','\n');
        Id key=keyMapper.map(res.first);
        Id value=valueMapper.map(res.second);
         index->insert(key, value);
      }
   }
};

template<class TargetIndex>
struct UniqueMappingIndex_FinishChunks {
   TargetIndex** targetPtr;
   TargetIndex* index;
   io::MmapedFile* file;
   ChunkTokenizer* chunks;
   UniqueMappingIndex_FinishChunks(TargetIndex** targetPtr, TargetIndex* index, io::MmapedFile* file, ChunkTokenizer* chunks)
      : targetPtr(targetPtr), index(index), file(file), chunks(chunks)
   { }

   void operator()() {
      *targetPtr=index;
      delete chunks;
      delete file;
   }
};

template<class TargetIndex, class KeyMapper, class ValueMapper, bool parallel=false>
TaskGroup uniqueMappingIndex(TaskGraph::Node node, TargetIndex** targetPtr, const string& path, KeyMapper& keyMapper, ValueMapper& valueMapper, uint32_t avgLineLength) {

   TargetIndex* index = new TargetIndex();
   io::MmapedFile* file=new io::MmapedFile(path,O_RDONLY);
   madvise(reinterpret_cast<void*>(file->mapping),file->size,MADV_WILLNEED);

   auto pairCount=file->size/avgLineLength;
   index->allocate(pairCount);

   // Split file into tasks
   tokenize::Tokenizer tokenizer(*file);
   tokenizer.skipAfter('\n'); //Skip header
   size_t maxChunks=parallel?512:1;
   size_t chunkSize=1<<22;
   ChunkTokenizer* chunks = new ChunkTokenizer(tokenizer, chunkSize, maxChunks);

   TaskGroup readTasks;
   size_t numChunks=chunks->getNumChunks();
   for (signed i=numChunks-1;i>=0;--i) {
      readTasks.schedule(LambdaRunner::createLambdaTask(UniqueMappingIndex_ReadChunk<TargetIndex,KeyMapper,ValueMapper>(i,chunks,keyMapper,valueMapper,index), node));
   }

   readTasks.join(LambdaRunner::createLambdaTask(UniqueMappingIndex_FinishChunks<TargetIndex>(targetPtr, index, file, chunks), node));
   return readTasks;
}

const size_t keyValuesBlockSize = 1024*1024;

/// If countItems is true, the number of values is returned, if not, 0
template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair, bool notLastValue, bool countItems=false, bool collectValues=false, bool filterKeys=false>
size_t loadUnsortedListsIntoIndex(TargetIndex* index, tokenize::Tokenizer& tokenizer, KeyMapper& keyMapper, ValueMapper& valueMapper, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut, const unordered_set<typename TargetIndex::Id>& keyFilter) {
   typedef typename TargetIndex::Id KeyType;
   typedef typename std::remove_pointer<typename TargetIndex::Content>::type TargetIndexContent;
   typedef typename TargetIndexContent::Entry ValueType;
   typedef typename TargetIndexContent::Size SizeType;
   typedef SizedList<SizeType, ValueType> SizedListType;
   typedef LinkedSizedList<SizeType, ValueType> LinkedSizedListType;

   size_t numItems=0;

   KeyType curKey = numeric_limits<KeyType>::max();
   char* curKeyValuesStart=nullptr;
   size_t numBlockValues=0;
   char* valuesBlockEnd = nullptr;
   char* valuesBlockPos = nullptr;
   LinkedSizedListType* curKeyDestList=nullptr;

   struct FinalizeKey {
      TargetIndex* index;
      size_t& numBlockValues;
      LinkedSizedListType*& curKeyDestList;
      char*& valuesBlockPos;
      char*& valuesBlockEnd;
      char*& curKeyValuesStart;
      KeyType& curKey;
      size_t& numItems;

      FinalizeKey(TargetIndex* index, size_t& numBlockValues, LinkedSizedListType*& curKeyDestList, char*& valuesBlockPos, char*& valuesBlockEnd, char*& curKeyValuesStart, KeyType& curKey, size_t& numItems)
         : index(index), numBlockValues(numBlockValues), curKeyDestList(curKeyDestList), valuesBlockPos(valuesBlockPos), valuesBlockEnd(valuesBlockEnd), curKeyValuesStart(curKeyValuesStart), curKey(curKey), numItems(numItems)
      { }

      void operator()(KeyType key) {
         if(numBlockValues>0) {
         //Check if there is already a value list for this key
            if(curKeyDestList == nullptr) {
            //No list yet, create one
               LinkedSizedListType::create(numBlockValues, curKeyValuesStart);
               index->insert(curKey, reinterpret_cast<TargetIndexContent*>(curKeyValuesStart));
            } else {
            //List does exist. Append list
               curKeyDestList->appendList(numBlockValues, curKeyValuesStart);
            }
            valuesBlockPos += sizeof(SizedListType*);

            if(countItems) {
               numItems += numBlockValues;
            }
         }
         curKey = key;

         //Allocate memory block if necessary
         if((valuesBlockPos == nullptr)||(valuesBlockPos+sizeof(LinkedSizedListType)+sizeof(SizeType)+2*sizeof(ValueType)+sizeof(SizedListType*)>=valuesBlockEnd)) {
            valuesBlockPos = new char[keyValuesBlockSize];
            valuesBlockEnd = valuesBlockPos + keyValuesBlockSize;
         }
         curKeyValuesStart = valuesBlockPos;

         curKeyDestList = reinterpret_cast<LinkedSizedListType*>(index->retrieve(key));
         const auto reservedFieldLengths =  curKeyDestList == nullptr ? sizeof(LinkedSizedListType)+sizeof(SizeType) : sizeof(SizeType);
         valuesBlockPos += reservedFieldLengths;
         numBlockValues = 0;
      }
   };
   FinalizeKey finalizeKey(index, numBlockValues, curKeyDestList, valuesBlockPos, valuesBlockEnd, curKeyValuesStart, curKey, numItems);

   if(!filterKeys) {
      //No filter
      if (notLastValue) {
         //Not last value
         if (!reversePair) {
            //Not reverse
            while(!tokenizer.finished()) {
               const auto result=tokenizer.consumeLongLongSingleDelimiterCacheFirst('|');
               const auto key=keyMapper.map(result.first);
               const ValueType value=valueMapper.map(result.second);
               tokenizer.skipAfter('\n');

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         } else {
            //Reverse
            while(!tokenizer.finished()) {
               const auto result=tokenizer.consumeLongLongSingleDelimiter('|');
               const auto key=keyMapper.map(result.second);
               const ValueType value=valueMapper.map(result.first);
               tokenizer.skipAfter('\n');

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         }
      } else {
         //Last value
         if (!reversePair) {
            //Not reverse
            while(!tokenizer.finished()) {
               const auto result=tokenizer.consumeLongLongDistinctDelimiterCacheFirst('|','\n');
               const auto key=keyMapper.map(result.first);
               const ValueType value=valueMapper.map(result.second);

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         } else {
            //Reverse
            while(!tokenizer.finished()) {
               const auto result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
               const auto key=keyMapper.map(result.second);
               const ValueType value=valueMapper.map(result.first);

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         }
      }
   } else {
      //Filter keys
      if (notLastValue) {
         //Not last value
         if (!reversePair) {
            //Not reverse
            while(!tokenizer.finished()) {
               auto key=keyMapper.map(tokenizer.consumeLong('|'));
               if(keyFilter.find(key)==keyFilter.end()) {
                  finalizeKey(key); //Finalize previous key
                  //Skip all filtered entries
                  do {
                     const auto filteredKey = key;
                     do {
                        tokenizer.skipAfter('\n');
                        if(unlikely(tokenizer.finished())) { break; }
                        key=keyMapper.map(tokenizer.consumeLong('|'));
                     } while(key==filteredKey);
                  } while(keyFilter.find(key)==keyFilter.end() && likely(!tokenizer.finished()));
                  if(unlikely(tokenizer.finished())) { break; }
               }
               const ValueType value=valueMapper.map(tokenizer.consumeLong('|'));

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
               tokenizer.skipAfter('\n');
            }
         } else {
            //Reverse
            while(!tokenizer.finished()) {
               auto result=tokenizer.consumeLongLongSingleDelimiter('|');
               auto key=keyMapper.map(result.second);

               if(keyFilter.find(key)==keyFilter.end()) {
                  finalizeKey(key); //Finalize previous key
                  //Skip all filtered entries
                  do {
                     const auto filteredKey = key;
                     do {
                        tokenizer.skipAfter('\n');
                        if(unlikely(tokenizer.finished())) { break; }
                        result=tokenizer.consumeLongLongSingleDelimiter('|');
                        key=keyMapper.map(result.second);
                     } while(likely(!tokenizer.finished()) && key==filteredKey);
                  } while(keyFilter.find(key)==keyFilter.end() && likely(!tokenizer.finished()));
                  if(unlikely(tokenizer.finished())) { break; }
               }
               const ValueType value=valueMapper.map(result.first);

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
               tokenizer.skipAfter('\n');
            }
         }
      } else {
         //Last value
         if (!reversePair) {
            //Not reverse
            while(!tokenizer.finished()) {
               auto key=keyMapper.map(tokenizer.consumeLong('|'));
               if(keyFilter.find(key)==keyFilter.end()) {
                  finalizeKey(key); //Finalize previous key
                  //Skip all filtered entries
                  do {
                     const auto filteredKey = key;
                     do {
                        tokenizer.skipAfter('\n');
                        if(unlikely(tokenizer.finished())) { break; }
                        key=keyMapper.map(tokenizer.consumeLong('|'));
                     } while(key==filteredKey);
                  } while(keyFilter.find(key)==keyFilter.end() && likely(!tokenizer.finished()));
                  if(unlikely(tokenizer.finished())) { break; }
               }
               const ValueType value=valueMapper.map(tokenizer.consumeLong('\n'));

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         } else {
            //Reverse
            while(!tokenizer.finished()) {
               auto result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
               auto key=keyMapper.map(result.second);

               if(keyFilter.find(key)==keyFilter.end()) {
                  finalizeKey(key); //Finalize previous key
                  //Skip all filtered entries
                  do {
                     const auto filteredKey = key;
                     do {
                        if(unlikely(tokenizer.finished())) { break; }
                        result=tokenizer.consumeLongLongDistinctDelimiter('|','\n');
                        key=keyMapper.map(result.second);
                     } while(key==filteredKey);
                  } while(keyFilter.find(key)==keyFilter.end() && likely(!tokenizer.finished()));
                  if(unlikely(tokenizer.finished())) { break; }
               }
               const ValueType value=valueMapper.map(result.first);

               if(key != curKey || unlikely(valuesBlockPos+sizeof(ValueType)+sizeof(SizedListType*)>valuesBlockEnd)) {
                  finalizeKey(key);
               }
               *reinterpret_cast<ValueType*>(valuesBlockPos) = value;
               valuesBlockPos += sizeof(ValueType);
               ++numBlockValues;

               if(collectValues) {
                  valuesOut.insert(value);
               }
            }
         }
      }
   }
   finalizeKey(numeric_limits<KeyType>::max()-1);

   return numItems;
}

template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair, bool notLastValue, bool collectValues, bool filterKeys>
struct UnsortedGroupingIndex_BuildSequential {
   typedef KeyMapper IndexKeyMapper;
   typedef ValueMapper IndexValueMapper;

   io::MmapedFile* const file;
   const TargetIndex** const targetPtr;
   KeyMapper& keyMapper;
   const uint32_t numKeys;
   ValueMapper& valueMapper;
   unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut;
   const unordered_set<typename TargetIndex::Id>& keyFilter;

   UnsortedGroupingIndex_BuildSequential(io::MmapedFile* file, const TargetIndex** targetPtr, KeyMapper& keyMapper, uint32_t numKeys, ValueMapper& valueMapper, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut, const unordered_set<typename TargetIndex::Id>& keyFilter)
      : file(file), targetPtr(targetPtr), keyMapper(keyMapper), numKeys(numKeys), valueMapper(valueMapper), valuesOut(valuesOut), keyFilter(keyFilter)
   { }

   void operator()() {
      tokenize::Tokenizer tokenizer(*file);
      tokenizer.skipAfter('\n'); //Skip header

      TargetIndex* index = new TargetIndex(numKeys);
      loadUnsortedListsIntoIndex<TargetIndex, KeyMapper, ValueMapper, reversePair, notLastValue, false /*countItems*/, collectValues, filterKeys>(index, tokenizer, keyMapper, valueMapper, valuesOut, keyFilter);
      *targetPtr = index;

      delete file;
   }
};

template<class TargetIndex>
struct GroupingIndex_ParallelChunkData {
   TargetIndex* index;
   unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry> values;
   size_t numVals;

   GroupingIndex_ParallelChunkData(TargetIndex* index)
      : index(index), numVals(0)
   { }
};

template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair, bool notLastValue, bool countItems, bool collectValues, bool filterKeys>
struct GroupingIndex_BuildParallel {
   const size_t c;
   ChunkTokenizer* const chunkTokenizer;
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const chunks;
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const unusedChunks;
   mutex* const chunkMutex;
   KeyMapper& keyMapper;
   uint32_t numKeys;
   ValueMapper& valueMapper;
   const unordered_set<typename TargetIndex::Id>& keyFilter;

   GroupingIndex_BuildParallel(size_t c, ChunkTokenizer* chunkTokenizer, vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* chunks, vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* unusedChunks, mutex* chunkMutex, KeyMapper& keyMapper, uint32_t numKeys, ValueMapper& valueMapper, const unordered_set<typename TargetIndex::Id>& keyFilter)
      : c(c), chunkTokenizer(chunkTokenizer), chunks(chunks), unusedChunks(unusedChunks), chunkMutex(chunkMutex), keyMapper(keyMapper), numKeys(numKeys), valueMapper(valueMapper), keyFilter(keyFilter)
   { }

   void operator()() {
      typedef typename std::remove_pointer<typename TargetIndex::Content>::type TargetIndexContent;
      typedef typename TargetIndexContent::Entry ValueType;

      //Get existing chunk data or create new
      GroupingIndex_ParallelChunkData<TargetIndex>* chunkData;
      {
         lock_guard<mutex> lock(*chunkMutex);
         if(!unusedChunks->empty()) {
            chunkData = unusedChunks->back();
            unusedChunks->pop_back();
         } else {
            chunkData = new GroupingIndex_ParallelChunkData<TargetIndex>(new TargetIndex(numKeys));
            chunks->push_back(chunkData);
         }
      }

      tokenize::Tokenizer innerTokenizer=chunkTokenizer->getTokenizer(c);
      const size_t numVals = loadUnsortedListsIntoIndex<TargetIndex, KeyMapper, ValueMapper, reversePair, notLastValue, countItems, collectValues, filterKeys>(chunkData->index, innerTokenizer, keyMapper, valueMapper, chunkData->values, keyFilter);
      if(countItems) {
         chunkData->numVals += numVals;
      }

      {
         lock_guard<mutex> lock(*chunkMutex);
         unusedChunks->push_back(chunkData);
      }
   }
};

template<class TargetIndex, bool collectValues>
struct UnsortedGroupingIndex_JoinParallel {
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const chunks;
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const unusedChunks;
   mutex* const chunkMutex;
   ChunkTokenizer* const chunkTokenizer;
   const uint32_t numKeys;
   const TargetIndex** const targetPtr;
   unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut;

   UnsortedGroupingIndex_JoinParallel(vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const chunks, vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const unusedChunks, mutex* const chunkMutex, ChunkTokenizer* const chunkTokenizer, uint32_t numKeys, const TargetIndex** const targetPtr, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut)
      : chunks(chunks), unusedChunks(unusedChunks), chunkMutex(chunkMutex), chunkTokenizer(chunkTokenizer), numKeys(numKeys), targetPtr(targetPtr), valuesOut(valuesOut)
   { }

   void operator()() {
      const auto numChunks=chunks->size();
      assert(numChunks>0);

      //Merge other chunks into first one
      GroupingIndex_ParallelChunkData<TargetIndex>& targetData = *chunks->operator[](0);
      for(size_t c=1; c<numChunks; c++) {
         //Merge index
         GroupingIndex_ParallelChunkData<TargetIndex>& mergeData = *chunks->operator[](c);
         targetData.index->mergeWith(mergeData.index);

         //Merge values
         if(collectValues) {
            for(auto vIter=mergeData.values.cbegin(); vIter!=mergeData.values.cend(); vIter++) {
               targetData.values.insert(*vIter);
            }
         }
      }
      *targetPtr = targetData.index;
      if(collectValues) {
         valuesOut = std::move(targetData.values);
      }

      //Clean up
      for(size_t c=0; c<numChunks; c++) {
         delete chunks->operator[](c);
      }

      delete chunks;
      delete unusedChunks;
      delete chunkMutex;
      delete chunkTokenizer;
   }
};

//, class KeyMapper, class ValueMapper, bool reversePair=false, bool notLastValue=false, bool parallel=false, bool collectValues=false, bool filterKeys=false
template<class TargetIndex, class SequentialBuilder, class ParallelBuilder, class ParallelJoiner, bool parallel>
TaskGroup groupingIndex(TaskGraph::Node node, const TargetIndex** targetPtr, const string& path, typename SequentialBuilder::IndexKeyMapper& keyMapper, uint32_t numKeys, typename SequentialBuilder::IndexValueMapper& valueMapper, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut, const unordered_set<typename TargetIndex::Id>& keyFilter=unordered_set<typename TargetIndex::Id>()) {

   // using KeyType = typename TargetIndex::Id;
   typedef typename std::remove_pointer<typename TargetIndex::Content>::type TargetIndexContent;
   typedef typename TargetIndexContent::Entry ValueType;
   typedef typename TargetIndexContent::Size SizeType;
   typedef LinkedSizedList<SizeType, ValueType> LinkedSizedListType;

   io::MmapedFile* file=new io::MmapedFile(path,O_RDONLY);
   madvise(reinterpret_cast<void*>(file->mapping),file->size,MADV_WILLNEED);

   TaskGroup readTasks;
   if(!parallel) {
      readTasks.schedule(LambdaRunner::createLambdaTask(SequentialBuilder(file, targetPtr, keyMapper, numKeys, valueMapper, valuesOut, keyFilter), node));
   } else {
      tokenize::Tokenizer tokenizer(*file);
      tokenizer.skipAfter('\n'); //Skip header

      //Construct chunks
      const size_t maxChunks = 512;
      const size_t chunkSize=1<<22;
      ChunkTokenizer* chunkTokenizer = new ChunkTokenizer(tokenizer, chunkSize, maxChunks);
      const size_t numChunks=chunkTokenizer->getNumChunks();
      auto chunks = new vector<GroupingIndex_ParallelChunkData<TargetIndex>*>();
      auto unusedChunks = new vector<GroupingIndex_ParallelChunkData<TargetIndex>*>();
      mutex* chunkMutex = new mutex();
      chunks->reserve(numChunks);

      //Schedule chunks
      for(size_t c=0; c<numChunks; c++) {
         readTasks.schedule(LambdaRunner::createLambdaTask(ParallelBuilder(c, chunkTokenizer, chunks, unusedChunks, chunkMutex, keyMapper, numKeys, valueMapper, keyFilter), node));
      }

      //Schedule result merging
      readTasks.join(LambdaRunner::createLambdaTask(ParallelJoiner(chunks,unusedChunks,chunkMutex,chunkTokenizer,numKeys,targetPtr,valuesOut), node));
   }

   return readTasks;
}

template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair=false, bool notLastValue=false, bool parallel=false, bool collectValues=false, bool filterKeys=false>
TaskGroup unsortedGroupingIndex(TaskGraph::Node node, const TargetIndex** targetPtr, const string& path, KeyMapper& keyMapper, uint32_t numKeys, ValueMapper& valueMapper, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>& valuesOut, const unordered_set<typename TargetIndex::Id>& keyFilter=unordered_set<typename TargetIndex::Id>()) {
   return groupingIndex<
   TargetIndex,
   UnsortedGroupingIndex_BuildSequential<TargetIndex,KeyMapper,ValueMapper,reversePair,notLastValue,collectValues,filterKeys>,
   GroupingIndex_BuildParallel<TargetIndex,KeyMapper,ValueMapper,reversePair,notLastValue,false,collectValues,filterKeys>,
   UnsortedGroupingIndex_JoinParallel<TargetIndex,collectValues>,
   parallel>(node, targetPtr, path, keyMapper, numKeys, valueMapper, valuesOut, keyFilter);
}

template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair, bool notLastValue>
struct SortedGroupingIndex_BuildSequential {
   typedef KeyMapper IndexKeyMapper;
   typedef ValueMapper IndexValueMapper;

   io::MmapedFile* file;
   const TargetIndex** targetPtr;
   KeyMapper& keyMapper;
   uint32_t numKeys;
   ValueMapper& valueMapper;

   SortedGroupingIndex_BuildSequential(io::MmapedFile* file, const TargetIndex** targetPtr, KeyMapper& keyMapper, uint32_t numKeys, ValueMapper& valueMapper, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>&, const unordered_set<typename TargetIndex::Id>&)
      : file(file), targetPtr(targetPtr), keyMapper(keyMapper), numKeys(numKeys), valueMapper(valueMapper)
   { }

   void operator()() {
      typedef typename TargetIndex::Id KeyType;
      typedef typename std::remove_pointer<typename TargetIndex::Content>::type TargetIndexContent;
      typedef typename TargetIndexContent::Entry ValueType;
      typedef typename TargetIndexContent::Size SizeType;
      typedef SizedList<SizeType, ValueType> SizedListType;
      typedef LinkedSizedList<SizeType, ValueType> LinkedSizedListType;

      tokenize::Tokenizer tokenizer(*file);
      tokenizer.skipAfter('\n'); //Skip header

      //Load file into unsorted lists
      TargetIndex* index = new TargetIndex(numKeys);
      unordered_set<KeyType> filterDummy;
      unordered_set<ValueType> valuesDummy;
      const size_t numVals = loadUnsortedListsIntoIndex<TargetIndex, KeyMapper, ValueMapper, reversePair, notLastValue, true>(index, tokenizer, keyMapper, valueMapper, valuesDummy, filterDummy);

      //Turn linked sized lists in index into pointers to final data
      const size_t requiredSpace = numKeys*sizeof(SizeType) + numVals*sizeof(ValueType);
      char* const data = new char[requiredSpace];

      index->buffer.data=data;
      index->buffer.size=requiredSpace;

      char* dataPos = data;
      const KeyType maxKey=index->maxKey();

      for(KeyType k=0; k<=maxKey; k++) {
         //Copy all values for this key to data
         LinkedSizedListType* valueLists = reinterpret_cast<LinkedSizedListType*>(index->retrieve(k));
         if(valueLists!=nullptr) {
            size_t numKeyValues = 0;
            SizedListType* listPtr = reinterpret_cast<SizedListType*>(dataPos);
            dataPos+=sizeof(SizeType);

            SizedListType* values = valueLists->firstList();
            do {
               const auto numListValues=values->size();
               memcpy(dataPos, values->getPtr(0), numListValues*sizeof(ValueType));
               numKeyValues += numListValues;
               dataPos += numListValues*sizeof(ValueType);
            } while((values=valueLists->nextList(values)) != nullptr);

            assert(dataPos <= data+requiredSpace);

            //Sort list
            sort(listPtr->getPtr(0), listPtr->getPtr(0)+numKeyValues);

            listPtr->setSize(numKeyValues);
            index->insert(k, listPtr);


            assert(index->retrieve(k)->size()==numKeyValues);
         }
      }

      *targetPtr = index;
   }
};

template<class TargetIndex>
struct SortedGroupingIndex_JoinParallel {
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const chunks;
   vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const unusedChunks;
   mutex* const chunkMutex;
   ChunkTokenizer* const chunkTokenizer;
   const uint32_t numKeys;
   const TargetIndex** const targetPtr;


   SortedGroupingIndex_JoinParallel(vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const chunks, vector<GroupingIndex_ParallelChunkData<TargetIndex>*>* const unusedChunks, mutex* const chunkMutex, ChunkTokenizer* const chunkTokenizer, uint32_t numKeys, const TargetIndex** const targetPtr, unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry>&)
   : chunks(chunks), unusedChunks(unusedChunks), chunkMutex(chunkMutex), chunkTokenizer(chunkTokenizer), numKeys(numKeys), targetPtr(targetPtr)
   { }

   void operator()() {
      typedef typename TargetIndex::Id KeyType;
      typedef typename std::remove_pointer<typename TargetIndex::Content>::type TargetIndexContent;
      typedef typename TargetIndexContent::Entry ValueType;
      typedef typename TargetIndexContent::Size SizeType;
      typedef SizedList<SizeType, ValueType> SizedListType;
      typedef LinkedSizedList<SizeType, ValueType> LinkedSizedListType;

      const auto numChunks=chunks->size();
      assert(numChunks>0);

      //Calculate sum of values
      size_t numVals=0;
      KeyType maxKey=0;
      for(size_t c=0; c<numChunks; c++) {
         //Merge index
         const GroupingIndex_ParallelChunkData<TargetIndex>& mergeData = *chunks->operator[](c);
         maxKey = max(maxKey, mergeData.index->maxKey());
         numVals += mergeData.numVals;
      }

      //Turn linked sized lists in index into pointers to final data
      const size_t requiredSpace = numKeys*sizeof(SizeType) + numVals*sizeof(ValueType);
      char* const data = new char[requiredSpace];

      TargetIndex* indexOut = new TargetIndex(numKeys);
      indexOut->buffer.data=data;
      indexOut->buffer.size=requiredSpace;

      char* dataPos = data;
      for(KeyType k=0; k<=maxKey; k++) {
         //Copy all values for this key to data
         size_t numKeyValues = 0;
         SizedListType* listPtr = reinterpret_cast<SizedListType*>(dataPos);
         dataPos+=sizeof(SizeType);

         for(size_t c=0; c<numChunks; c++) {
            LinkedSizedListType* valueLists = reinterpret_cast<LinkedSizedListType*>(chunks->operator[](c)->index->retrieve(k));
            if(valueLists!=nullptr) {
               SizedListType* values = valueLists->firstList();
               do {
                  const auto numListValues=values->size();
                  memcpy(dataPos, values->getPtr(0), numListValues*sizeof(ValueType));

                  numKeyValues += numListValues;
                  dataPos += numListValues*sizeof(ValueType);
               } while((values=valueLists->nextList(values)) != nullptr);
            }
         }
         assert(dataPos <= data+requiredSpace);

         //Sort list
         sort(listPtr->getPtr(0), listPtr->getPtr(0)+numKeyValues);

         listPtr->setSize(numKeyValues);
         indexOut->insert(k, listPtr);


         assert(indexOut->retrieve(k)->size()==numKeyValues);
      }

      *targetPtr = indexOut;

      //Clean up
      for(size_t c=0; c<numChunks; c++) {
         delete chunks->operator[](c)->index;
         delete chunks->operator[](c);
      }

      delete chunks;
      delete unusedChunks;
      delete chunkMutex;
      delete chunkTokenizer;
   }
};

template<class TargetIndex, class KeyMapper, class ValueMapper, bool reversePair=false, bool notLastValue=false, bool parallel=false>
TaskGroup sortedGroupingIndex(TaskGraph::Node node, const TargetIndex** targetPtr, const string& path, KeyMapper& keyMapper, uint32_t numKeys, ValueMapper& valueMapper) {
   unordered_set<typename std::remove_pointer<typename TargetIndex::Content>::type::Entry> valuesOut;
   return groupingIndex<
            TargetIndex,
            SortedGroupingIndex_BuildSequential<TargetIndex,KeyMapper,ValueMapper,reversePair,notLastValue>,
            GroupingIndex_BuildParallel<TargetIndex,KeyMapper,ValueMapper,reversePair,notLastValue,true,false,false>,
            SortedGroupingIndex_JoinParallel<TargetIndex>,
            parallel>
            (node, targetPtr, path, keyMapper, numKeys, valueMapper, valuesOut);
}


template <class PersonMapper,bool parallel=true>
TaskGroup schedulePersonGraph(const PersonGraph** targetPtr, const string& dataPath, PersonMapper& mapper) {
   return sortedGroupingIndex<PersonGraph, PersonMapper, PersonMapper, false, false, parallel>(TaskGraph::PersonGraph, targetPtr, dataPath+CSVFiles::PersonGraph, mapper, mapper.count(), mapper);
}

template <class CommentMapper, class PersonMapper>
TaskGroup buildCommentCreatorMap(CommentCreatorMap** targetPtr, const string& dataPath, CommentMapper& commentMapper, PersonMapper& personMapper) {
   metrics::BlockStats<>::LogSensor sensor("commentCreators");
   return uniqueMappingIndex<CommentCreatorMap, CommentMapper, PersonMapper, true>(TaskGraph::CommentCreatorMap, targetPtr, dataPath+CSVFiles::CommentCreatorGraph, commentMapper, personMapper, 8);
}

TaskGroup scheduleHasInterestIndex(const HasInterestIndex** targetPtr, const string& dataDir, PersonMapper& mapper);

TaskGroup scheduleTagInForumsIndex(const HashIndex<InterestId,LinkedSizedList<uint32_t,ForumId>*>** targetPtr, const string& dataDir);

TaskGroup scheduleHasMemberIndex(const HasMemberIndex** targetPtr,const string& dataDir, PersonMapper& mapper, const unordered_set<ForumId>& usedForums);

TagIndex* buildTagIndex(const string& dataPath, const unordered_set<awfy::StringRef>& usedTags);

PlaceBoundsIndex buildPlaceBoundsIndex(const string& dataDir);

PersonPlaceIndex buildPersonPlacesIndex(const string& dataDir, PersonMapper& personMapper, const PlaceBoundsIndex& boundsIndex);

NamePlaceIndex buildNamePlaceIndex(const string& dataDir);
