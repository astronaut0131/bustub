#pragma once

#include "common/config.h"

namespace bustub {

// Document this class! What does it represent?

// when doing hash join
// we create a temp page to store all the tuple from left table which corresponds to the input key from right tuple
// so that we can refer to this page when calling Next from the hash join executor
class TmpTuple {
 public:
  TmpTuple(page_id_t page_id, size_t offset) : page_id_(page_id), offset_(offset) {}
  TmpTuple() = default;
  inline bool operator==(const TmpTuple &rhs) const { return page_id_ == rhs.page_id_ && offset_ == rhs.offset_; }

  page_id_t GetPageId() const { return page_id_; }
  size_t GetOffset() const { return offset_; }

 private:
  page_id_t page_id_{};
  size_t offset_{};
};

}  // namespace bustub
