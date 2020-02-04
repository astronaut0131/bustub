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

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  header_ = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  header_->SetSize(std::ceil(num_buckets/BLOCK_ARRAY_SIZE) * BLOCK_ARRAY_SIZE);
  for (size_t i = 0; i < std::ceil(num_buckets / BLOCK_ARRAY_SIZE); i++) {
    page_id_t new_page_id;
    buffer_pool_manager->NewPage(&new_page_id);
    buffer_pool_manager->UnpinPage(new_page_id, true);
    header_->AddBlockPageId(new_page_id);
  }
  // flush it, header page content doesn't change until resizing
  // keep the header page in memory
  buffer_pool_manager_->FlushPage(header_page_id_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto offset_pair = CalculateOffset(key,GetSize());
  {
    auto cursor = Cursor(this, offset_pair.first, offset_pair.second, buffer_pool_manager_,header_->GetBlockPageIds(),header_->NumBlocks());
    auto block = *cursor;
    auto in_block_slot = cursor.GetInBlockSlot();
    while (block->IsOccupied(in_block_slot)) {
      if (block->IsReadable(in_block_slot) && comparator_(key, block->KeyAt(in_block_slot)) == 0)
        result->emplace_back(block->ValueAt(in_block_slot));
      cursor.Step();
      if (cursor.IsEnd()) break;
      in_block_slot = cursor.GetInBlockSlot();
      block = *cursor;
    }
  }
  table_latch_.RUnlock();
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto offset_pair = CalculateOffset(key,GetSize());
  {
    auto cursor = Cursor(this, offset_pair.first, offset_pair.second, buffer_pool_manager_,header_->GetBlockPageIds(),header_->NumBlocks());
    auto block = *cursor;
    auto in_block_slot = cursor.GetInBlockSlot();
    while (block->IsOccupied(in_block_slot) && block->IsReadable(in_block_slot)) {
      // replicated key value pair should fail to insert
      if (comparator_(block->KeyAt(in_block_slot), key) == 0 && block->ValueAt(in_block_slot) == value) {
        table_latch_.RUnlock();
        return false;
      }
      cursor.Step();
      if (cursor.IsEnd()) break;
      in_block_slot = cursor.GetInBlockSlot();
      block = *cursor;
    }
    if (!cursor.IsEnd()) {
      // can find a position to insert
      cursor.StartWrite();
      bool ret = block->Insert(in_block_slot, key, value);
      cursor.EndWrite();
      table_latch_.RUnlock();
      return ret;
    }
  }
  // should resize
  // resize and retry the insert
  table_latch_.RUnlock();
  Resize(GetSize());
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto offset_pair = CalculateOffset(key,GetSize());
  {
    auto cursor = Cursor(this, offset_pair.first, offset_pair.second, buffer_pool_manager_,header_->GetBlockPageIds(),header_->NumBlocks());
    auto block = *cursor;
    auto in_block_slot = cursor.GetInBlockSlot();
    while (block->IsOccupied(in_block_slot)) {
      if (block->IsReadable(in_block_slot) && comparator_(block->KeyAt(in_block_slot), key) == 0 &&
          block->ValueAt(in_block_slot) == value) {
        cursor.StartWrite();
        block->Remove(in_block_slot);
        cursor.EndWrite();
        table_latch_.RUnlock();
        return true;
      }
      cursor.Step();
      if (cursor.IsEnd()) break;
      in_block_slot = cursor.GetInBlockSlot();
      block = *cursor;
    }
  }
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  size_t new_size = std::ceil(2 * initial_size/BLOCK_ARRAY_SIZE);
  page_id_t new_pages[new_size];
  page_id_t new_page_id;
  // allocate new space
  for (size_t i = 0; i < new_size; i++) {
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr)
      throw std::runtime_error("fail to allocate new page while resizing LinearProbeHashTable");
    new_pages[i] = new_page_id;
    buffer_pool_manager_->UnpinPage(new_page_id,true);
  }
  auto cursor = Cursor(this,0,0,buffer_pool_manager_,header_->GetBlockPageIds(),header_->NumBlocks());
  slot_offset_t in_block_slot;
  // rehash all key-value pairs
  while (!cursor.IsEnd()) {
    auto block = *cursor;
    in_block_slot = cursor.GetInBlockSlot();
    if (block->IsOccupied(in_block_slot) && block->IsReadable(in_block_slot)) {
      auto key = block->KeyAt(in_block_slot);
      auto value = block->ValueAt(in_block_slot);
      // insert key-value to the newly allocated buckets
      auto offset_pair = CalculateOffset(key,new_size*BLOCK_ARRAY_SIZE);
      {
        auto new_cursor =
            Cursor(nullptr, offset_pair.first, offset_pair.second, buffer_pool_manager_, new_pages, new_size);
        auto new_block = *new_cursor;
        auto new_in_block_slot = new_cursor.GetInBlockSlot();
        while (new_block->IsOccupied(new_in_block_slot) && new_block->IsReadable(new_in_block_slot)) {
          new_cursor.Step();
          new_block = *new_cursor;
          new_in_block_slot = new_cursor.GetInBlockSlot();
        }
        new_block->Insert(new_in_block_slot,key,value);
      }
    }
    cursor.Step();
  }
  // deallocate the old pages
  for (size_t i = 0; i < header_->NumBlocks(); i++) {
    buffer_pool_manager_->DeletePage(header_->GetBlockPageId(i));
  }
  // copy new page ids
  for (size_t i = 0; i < new_size; i++) {
    header_->SetBlockPageId(i,new_pages[i]);
  }
  header_->SetSize(new_size * BLOCK_ARRAY_SIZE);
  header_->SetNextIndex(new_size);
  // flush the header_
  buffer_pool_manager_->FlushPage(header_page_id_);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return header_->NumBlocks() * BLOCK_ARRAY_SIZE;
}

/*****************************************************************************
 * HELPER FUNCTION
 *****************************************************************************/

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<slot_offset_t, slot_offset_t> HASH_TABLE_TYPE::CalculateOffset(const KeyType &key,size_t buckets_num) {
  auto hash_val = hash_fn_.GetHash(key);
  hash_val %= buckets_num;
  // pair.first: slot num in block_page_ids_ in hash_table_header_page
  // pair.second: slot num in array_ in hash_table_block_page
  return std::make_pair(hash_val / BLOCK_ARRAY_SIZE, hash_val % BLOCK_ARRAY_SIZE);
}

template class LinearProbeHashTable<int, int, IntComparator>;
template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
