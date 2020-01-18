//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.h
//
// Identification: src/include/container/hash/linear_probe_hash_table.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "container/hash/hash_table.h"
#include "storage/page/hash_table_block_page.h"
#include "storage/page/hash_table_header_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

#define HASH_TABLE_TYPE LinearProbeHashTable<KeyType, ValueType, KeyComparator>

/**
 * Implementation of linear probing hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table dynamically grows once full.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class LinearProbeHashTable : public HashTable<KeyType, ValueType, KeyComparator> {
 public:
  /**
   * Creates a new LinearProbeHashTable
   *
   * @param buffer_pool_manager buffer pool manager to be used
   * @param comparator comparator for keys
   * @param num_buckets initial number of buckets contained by this hash table
   * @param hash_fn the hash function
   */
  explicit LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator, size_t num_buckets, HashFunction<KeyType> hash_fn);

  /**
   * Inserts a key-value pair into the hash table.
   * @param transaction the current transaction
   * @param key the key to create
   * @param value the value to be associated with the key
   * @return true if insert succeeded, false otherwise
   */
  bool Insert(Transaction *transaction, const KeyType &key, const ValueType &value) override;

  /**
   * Deletes the associated value for the given key.
   * @param transaction the current transaction
   * @param key the key to delete
   * @param value the value to delete
   * @return true if remove succeeded, false otherwise
   */
  bool Remove(Transaction *transaction, const KeyType &key, const ValueType &value) override;

  /**
   * Performs a point query on the hash table.
   * @param transaction the current transaction
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @return the value(s) associated with the given key
   */
  bool GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) override;

  /**
   * Resizes the table to at least twice the initial size provided.
   * @param initial_size the initial size of the hash table
   */
  void Resize(size_t initial_size);

  /**
   * Gets the size of the hash table
   * @return current size of the hash table
   */
  size_t GetSize();

 private:
  // helper function
  HashTableHeaderPage* GetTableHeaderPtr() {
    return reinterpret_cast<HashTableHeaderPage*> (buffer_pool_manager_->FetchPage(header_page_id_));
  }
  HashTableBlockPage<KeyType,ValueType,KeyComparator>* GetTableBlockPtr(slot_offset_t page_offset) {
    page_id_t block_page_id = GetTableHeaderPtr()->GetBlockPageId(page_offset);
    return reinterpret_cast<HashTableBlockPage<KeyType,ValueType,KeyComparator>*> (buffer_pool_manager_->FetchPage(block_page_id));
  }

  std::pair<slot_offset_t,slot_offset_t> GetOffset(const KeyType& key) {
    auto hash_val = hash_fn_.GetHash(key);
    hash_val %= GetSize();
    return std::make_pair(hash_val/BLOCK_ARRAY_SIZE,hash_val%BLOCK_ARRAY_SIZE);
  }

  std::pair<slot_offset_t,slot_offset_t> NextBucket(const std::pair<slot_offset_t,slot_offset_t>& offset_pair) {
    slot_offset_t page_offset = offset_pair.first;
    slot_offset_t inblock_offset = offset_pair.second;
    if (inblock_offset < BLOCK_ARRAY_SIZE) return std::make_pair(page_offset,inblock_offset+1);
    else if (page_offset < GetTableHeaderPtr()->GetSize()) return std::make_pair(page_offset+1,0);
    else return std::make_pair(0,0);
  }

  std::pair<bool,bool> GetStatus(const std::pair<slot_offset_t,slot_offset_t>& offset_pair) {
    auto block_ptr = GetTableBlockPtr(offset_pair.first);
    return std::make_pair(block_ptr->IsOccupied(offset_pair.second),block_ptr->IsReadable(offset_pair.second));
  }

  std::pair<KeyType,ValueType> KeyValueAt(const std::pair<slot_offset_t,slot_offset_t>& offset_pair) {
    auto block_ptr = GetTableBlockPtr(offset_pair.first);
    return std::make_pair(block_ptr->KeyAt(offset_pair.second),block_ptr->ValueAt(offset_pair.second));
  }

  bool WriteKeyValue(const std::pair<slot_offset_t,slot_offset_t>& offset_pair,const KeyType& key, const ValueType& value) {
    auto block_ptr = GetTableBlockPtr(offset_pair.first);
    return block_ptr->Insert(offset_pair.second,key,value);
  }

  void RemoveKeyValue(const std::pair<slot_offset_t,slot_offset_t>& offset_pair) {
    auto block_ptr = GetTableBlockPtr(offset_pair.first);
    block_ptr->Remove(offset_pair.second);
  }
  // member variable
  page_id_t header_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  // Readers includes inserts and removes, writer is only resize
  ReaderWriterLatch table_latch_;

  // Hash function
  HashFunction<KeyType> hash_fn_;
};

}  // namespace bustub
