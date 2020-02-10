#pragma once

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  // similar code learned from table_page.h/cpp  :)
  void Init(page_id_t page_id, uint32_t page_size) {
    SetPageId(page_id);
    SetFreeSpacePointer(page_size);
  }

  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    auto free_offset = GetFreeSpacePointer();
    uint32_t need_size = sizeof(uint32_t) + tuple.GetLength();
    if (free_offset-need_size < sizeof(page_id_t) + sizeof(lsn_t) + sizeof(uint32_t)) return false;
    free_offset -= need_size;
    SetFreeSpacePointer(free_offset);
    auto addr = GetNextPosToInsert();
    uint32_t size = tuple.GetLength();
    memcpy(addr,&size,sizeof(size));
    auto data = tuple.GetData();
    memcpy(addr+sizeof(size),data,tuple.GetLength());
    *out = TmpTuple(GetTablePageId(),GetOffset());
    return true;
  }

  void Get(Tuple *tuple, size_t offset) {
    assert(offset >= sizeof(page_id_t) + sizeof(lsn_t) + sizeof(u_int32_t));
    tuple->DeserializeFrom(GetData()+offset);
  }

 private:
  static_assert(sizeof(page_id_t) == 4);
  inline void SetPageId(page_id_t page_id) { memcpy(GetData(),&page_id,sizeof(page_id)); }
  inline uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData()+sizeof(page_id_t)+sizeof(lsn_t)); }
  inline void SetFreeSpacePointer(uint32_t size) { memcpy(GetData()+sizeof(page_id_t)+sizeof(lsn_t),&size,sizeof(uint32_t)); }
  inline char* GetNextPosToInsert() { return GetData() + GetFreeSpacePointer(); }
  inline size_t GetOffset() { return GetFreeSpacePointer(); }
};

}  // namespace bustub
