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
  num_pages_ = static_cast<int>(num_pages);
  valid_.resize(num_pages);
  referenced_.resize(num_pages);
  fill(valid_.begin(), valid_.end(), false);
  fill(referenced_.begin(), referenced_.end(), false);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // no valid page in the clock replacer
  if (valid_num_ == 0) return false;
  while (true) {
    if (valid_[clock_hand_] && !referenced_[clock_hand_]) {
      // victim page
      valid_[clock_hand_] = false;
      valid_num_--;
      *frame_id = clock_hand_;
      Step();
      return true;
    } else if (valid_[clock_hand_]) {
      referenced_[clock_hand_] = false;
    }
    Step();
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id >= num_pages_ || !valid_[frame_id]) return;
  if (!referenced_[frame_id]) {
    referenced_[frame_id] = true;
    valid_[frame_id] = false;
    valid_num_--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id >= num_pages_) return;
  if (!valid_[frame_id]) {
    valid_[frame_id] = true;
    valid_num_++;
  }
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return valid_num_;
}
}  // namespace bustub
