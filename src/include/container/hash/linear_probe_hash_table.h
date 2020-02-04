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
  // helper class for traversing
  class Cursor {
   public:
    DISALLOW_COPY(Cursor);
    explicit Cursor(HASH_TABLE_TYPE *table_ptr, slot_offset_t block_slot, slot_offset_t in_block_slot,
                    BufferPoolManager *buffer_pool_manager,page_id_t* page_id_array,size_t page_id_array_size)
        : block_slot_(block_slot),
          in_block_slot_(in_block_slot),
          origin_block_slot_(block_slot),
          origin_in_block_slot_(in_block_slot),
          page_id_array_(page_id_array),
          page_id_array_size_(page_id_array_size),
          table_ptr_(table_ptr),
          buffer_pool_manager_(buffer_pool_manager) {
      assert(in_block_slot < BLOCK_ARRAY_SIZE);
      assert(block_slot < page_id_array_size);
      block_page_ = buffer_pool_manager->FetchPage(page_id_array_[block_slot]);
      block_page_->RLatch();
      block_ = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(block_page_->GetData());
    }
    // call it before modifying the page
    void StartWrite() {
      block_page_->RUnlatch();
      block_page_->WLatch();
    }
    // call it after modifying the page
    void EndWrite() {
      block_page_->SetDirty(true);
      block_page_->WUnlatch();
      block_page_->RLatch();
    }
    HASH_TABLE_BLOCK_TYPE* operator*() {
      return block_;
    }
    ~Cursor() {
      buffer_pool_manager_->UnpinPage(block_page_->GetPageId(),false);
      block_page_->RUnlatch();
    }
    bool IsEnd() {
      return has_stepped_ && block_slot_ == origin_block_slot_ && in_block_slot_ == origin_in_block_slot_;
    }
    void Step() {
      has_stepped_ = true;
      if (in_block_slot_ < BLOCK_ARRAY_SIZE - 1) {
        in_block_slot_++;
      } else {
        // switch to next block
        block_slot_++;
        if (block_slot_ == page_id_array_size_) block_slot_ = 0;
        in_block_slot_ = 0;
        if (IsEnd()) return;
        // release the last page
        block_page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(block_page_->GetPageId(), false);
        auto block_page_id = page_id_array_[block_slot_];
        block_page_ = buffer_pool_manager_->FetchPage(block_page_id);
        block_page_->RLatch();
        block_ = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_->GetData());
      }
    }
    slot_offset_t GetInBlockSlot() const { return in_block_slot_; }
    slot_offset_t GetBlockSlot() const { return block_slot_; }
   private:
    slot_offset_t block_slot_;
    slot_offset_t in_block_slot_;
    slot_offset_t origin_block_slot_;
    slot_offset_t origin_in_block_slot_;
    page_id_t* page_id_array_;
    size_t page_id_array_size_;
    HASH_TABLE_TYPE *table_ptr_;
    BufferPoolManager *buffer_pool_manager_;
    Page *block_page_{};
    HASH_TABLE_BLOCK_TYPE *block_{};
    bool has_stepped_ = false;
  };
  // helper function;

  std::pair<slot_offset_t, slot_offset_t> CalculateOffset(const KeyType &key,size_t buckets_num);

  // member variable
  page_id_t header_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  // Readers includes inserts and removes, writer is only resize
  ReaderWriterLatch table_latch_;

  // Hash function
  HashFunction<KeyType> hash_fn_;
  HashTableHeaderPage *header_;
};

}  // namespace bustub
