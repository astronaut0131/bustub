//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <include/common/logger.h>

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto it = page_table_.find(page_id);
  // the target page is already in the buffer pool
  if (it != page_table_.end()) return &pages_[it->second];
  frame_id_t victim_frame_id;
  if (!free_list_.empty()) {
    // try to find from free list first
    victim_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&victim_frame_id)) {
    return nullptr;
  }
  auto victim_page_ptr = &pages_[victim_frame_id];
  // write back page content if it is dirty
  if (victim_page_ptr->IsDirty()) disk_manager_->WritePage(victim_page_ptr->GetPageId(),victim_page_ptr->GetData());
  // update page table
  auto pair = page_table_.extract(victim_page_ptr->GetPageId());
  pair.key() = page_id;
  page_table_.insert(std::move(pair));
  // update page metadata
  victim_page_ptr->page_id_ = page_id;
  victim_page_ptr->pin_count_++;
  replacer_->Pin(victim_frame_id);
  disk_manager_->ReadPage(page_id,victim_page_ptr->data_);
  return victim_page_ptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || pages_[it->second].GetPinCount() <= 0) return false;
  pages_[it->second].pin_count_--;
  pages_[it->second].is_dirty_ = is_dirty;
  replacer_->Unpin(it->second);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return false;
  disk_manager_->WritePage(page_id,pages_[it->second].GetData());
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t target_frame_id;
  if (!free_list_.empty()) {
    target_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&target_frame_id)) return nullptr;
  auto& target_page = pages_[target_frame_id];
  if (target_page.IsDirty()) disk_manager_->WritePage(target_page.GetPageId(),target_page.GetData());
  // update metadata and zero out memory
  target_page.pin_count_ = 1;
  target_page.is_dirty_ = false;
  target_page.ResetMemory();
  // remove old page table entry
  auto it = page_table_.find(target_page.GetPageId());
  if (it != page_table_.end()) page_table_.erase(it);
  *page_id = target_page.page_id_ = disk_manager_->AllocatePage();
  // add new entry
  page_table_.emplace(std::make_pair(*page_id,target_frame_id));
  return &target_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return true;
  auto& page = pages_[it->second];
  if (page.pin_count_ > 0) return false;
  page_table_.erase(it);
  disk_manager_->DeallocatePage(page.page_id_);
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  replacer_->Pin(it->second);
  free_list_.emplace_back(static_cast<int>(it->second));
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto pair:page_table_) {
    disk_manager_->WritePage(pair.first,pages_[pair.second].GetData());
  }
}

}  // namespace bustub
