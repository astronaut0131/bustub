//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      buffer_pool_manager_->NewPage(&header_page_id_);
      auto header_page_ptr = GetTableHeaderPtr();
      header_page_ptr->SetSize(num_buckets/BLOCK_ARRAY_SIZE);
      for (size_t i = 0; i < num_buckets/BLOCK_ARRAY_SIZE; i++) {
        page_id_t new_page_id;
        buffer_pool_manager->NewPage(&new_page_id);
        header_page_ptr->AddBlockPageId(new_page_id);
      }
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto offset_pair = GetOffset(key);
  auto origin_offset_pair = offset_pair;
  auto status = GetStatus(offset_pair);
  // while bucket is occupied
  while(status.first) {
    // if readable
    if(status.second) {
      auto kv = KeyValueAt(offset_pair);
      //KeyComparator returns 0 when two keys are equal
      if (comparator_(key,kv.first) == 0) result->push_back(kv.second);
    }
    offset_pair = NextBucket(offset_pair);
    if (offset_pair == origin_offset_pair) break;
    status = GetStatus(offset_pair);
  }
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto offset_pair = GetOffset(key);
  auto origin_offset_pair = offset_pair;
  auto status = GetStatus(offset_pair);
  // stop if a bucket is not occupied or a tombstone
  while(status.first && status.second) {
    auto kv = KeyValueAt(offset_pair);
    // insert with existed kv should fail
    if (comparator_(key,kv.first) == 0 && value == kv.second) return false;
    offset_pair = NextBucket(offset_pair);
    status = GetStatus(offset_pair);
    // one round searched, the hash table is full
    if (offset_pair == origin_offset_pair) return false;
  }
  return WriteKeyValue(offset_pair,key,value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto offset_pair = GetOffset(key);
  auto origin_offset_pair = offset_pair;
  auto status = GetStatus(offset_pair);
  while (status.first) {
    // if the bucket is readable
    if (status.second) {
      auto kv = KeyValueAt(offset_pair);
      // kv to remove exists
      if (comparator_(key,kv.first) == 0 && value == kv.second) {
        RemoveKeyValue(offset_pair);
        return true;
      }
    }
    offset_pair = NextBucket(offset_pair);
    status = GetStatus(offset_pair);
    if (offset_pair == origin_offset_pair) return false;
  }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return GetTableHeaderPtr()->GetSize() * BLOCK_ARRAY_SIZE;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
