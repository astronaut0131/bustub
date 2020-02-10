//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return { array_[bucket_ind].first };
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return { array_[bucket_ind].second };
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if (IsOccupied(bucket_ind) && IsReadable(bucket_ind)) {
    return false;
  }
  SetOccupied(bucket_ind,true);
  SetReadable(bucket_ind,true);
  array_[bucket_ind] = std::make_pair(key,value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  // just mark it as a tombstone
  SetReadable(bucket_ind,false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  return GetBit(occupied_,bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  return GetBit(readable_,bucket_ind);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::GetBit(std::atomic_char *array,slot_offset_t index) const{
  auto slot = index/8;
  auto bit = index%8;
  char x = array[slot].load();
  return (x >> bit)&1UL;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetBit(std::atomic_char *array,slot_offset_t index,bool val) {
  auto slot = index/8;
  auto bit = index%8;
  char x = array[slot].load();
  if(val) x |= (1 << bit);
  else x &= ~(1 << bit);
  array[slot].store(x);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetOccupied(slot_offset_t index, bool val) {
  SetBit(occupied_,index,val);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetReadable(slot_offset_t index, bool val) {
  SetBit(readable_,index,val);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;
template class HashTableBlockPage<Value,Tuple,ValueComparator>;
}  // namespace bustub
