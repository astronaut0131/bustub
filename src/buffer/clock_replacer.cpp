//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

#include <include/common/logger.h>

#include <algorithm>

namespace bustub {
using std::fill;

ClockReplacer::ClockReplacer(size_t num_pages) {
  pages_.resize(num_pages);
  valid_.resize(num_pages);
  referenced_.resize(num_pages);
  fill(valid_.begin(), valid_.end(), false);
  fill(referenced_.begin(), referenced_.end(), false);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  // no valid page in the clock replacer
  if (valid_num_ == 0) return false;
  while (true) {
    if (valid_[clock_hand_] && !referenced_[clock_hand_]) {
      // victim page
      valid_[clock_hand_] = false;
      valid_num_--;
      size_--;
      *frame_id = pages_[clock_hand_];
      Step();
      return true;
    } else if (valid_[clock_hand_]) {
      referenced_[clock_hand_] = false;
    }
    Step();
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  auto it = find(pages_.begin(), pages_.end(), frame_id);
  int index = it - pages_.begin();
  if (it != pages_.end() && valid_[index] && !referenced_[index]) {
    referenced_[index] = true;
    size_--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  auto it = find(pages_.begin(), pages_.end(), frame_id);
  int index = it - pages_.begin();
  if (it == pages_.end()) {
    // find an invalid page_id position to put the new page_id in
    for (int i = 0; i < (int)valid_.size(); i++) {
      if (!valid_[i]) {
        pages_[i] = frame_id;
        valid_[i] = true;
        valid_num_++;
        size_++;
        return;
      }
    }
  } else if (!valid_[index]) {
    // this page_id is once made invalid
    valid_[index] = true;
    valid_num_++;
    size_++;
  } else if (referenced_[index]) {
    // turn this page_id from pinned to unpinned
    size_++;
  }
  // do nothing if this page_id is in pages_ and it's both valid and already unpinned
}

size_t ClockReplacer::Size() { return size_; }

}  // namespace bustub
