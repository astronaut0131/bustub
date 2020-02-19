//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
  try {
    memcpy((void *)log_record, data, LogRecord::HEADER_SIZE);
    if (log_record->GetLSN() == INVALID_LSN || log_record->GetTxnId() == INVALID_TXN_ID ||
        log_record->GetLogRecordType() == LogRecordType::INVALID) {
      return false;
    }
    auto pos = LogRecord::HEADER_SIZE;
    if (log_record->GetLogRecordType() == LogRecordType::INSERT) {
      memcpy(&log_record->insert_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(data + pos);
    } else if (log_record->GetLogRecordType() == LogRecordType::UPDATE) {
      memcpy(&log_record->update_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.DeserializeFrom(data + pos);
      pos += log_record->old_tuple_.GetLength();
      log_record->new_tuple_.DeserializeFrom(data + pos);
    } else if (log_record->GetLogRecordType() == LogRecordType::NEWPAGE) {
      memcpy(&log_record->prev_page_id_, data + pos, sizeof(page_id_t));
    } else if (log_record->log_record_type_ != LogRecordType::BEGIN &&
               log_record->log_record_type_ != LogRecordType::COMMIT &&
               log_record->log_record_type_ != LogRecordType::ABORT) {
      // deletion
      memcpy(&log_record->delete_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.DeserializeFrom(data + pos);
    }
    return true;
  } catch (...) {
    // return false if any exception raised during the deserialization
    return false;
  }
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  // traverse the log file
  // chances are disk_manager_->ReadLog truncates a log record at the tail of data it reads
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
    size_t pos = 0;
    LogRecord log_record;
    while (DeserializeLogRecord(log_buffer_ + pos, &log_record)) {
      // redo the action for each record
      if (log_record.GetLogRecordType() == LogRecordType::INSERT) {
        auto &rid = log_record.GetInsertRID();
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when redoing");
        // only redo when pageLSN < log.lsn
        // since pageLSN marks the latest update's lsn on the page
        // once it's durable on disk, the update corresponding to pageLSN is also durable on disk
        if (page->GetLSN() < log_record.GetLSN()) {
          page->WLatch();
          page->InsertTuple(log_record.GetInserteTuple(), &rid, nullptr, nullptr, nullptr);
          page->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      } else if (log_record.GetLogRecordType() == LogRecordType::UPDATE) {
        auto &rid = log_record.update_rid_;
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when redoing");
        if (page->GetLSN() < log_record.GetLSN()) {
          page->WLatch();
          page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
          page->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      } else if (log_record.GetLogRecordType() == LogRecordType::NEWPAGE) {
        page_id_t page_id;
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(&page_id));
        if (page == nullptr) throw std::runtime_error("fail to create a new page when redoing");
        page->Init(page_id, PAGE_SIZE, log_record.prev_page_id_, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        // set next page id for the prev page if the new page is not the first page
        if (log_record.prev_page_id_ != INVALID_PAGE_ID) {
          auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.prev_page_id_));
          if (prev_page == nullptr) throw std::runtime_error("fail to fetch prev page when reallocating new page");
          prev_page->WLatch();
          prev_page->SetNextPageId(page_id);
          prev_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true);
        }
      } else if (log_record.GetLogRecordType() == LogRecordType::MARKDELETE ||
                 log_record.GetLogRecordType() == LogRecordType::APPLYDELETE ||
                 log_record.GetLogRecordType() == LogRecordType::ROLLBACKDELETE) {
        auto& rid = log_record.GetDeleteRID();
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when redoing deletion");
        if (page->GetLSN() < log_record.GetLSN()) {
          page->WLatch();
          if (log_record.GetLogRecordType() == LogRecordType::MARKDELETE) {
            page->MarkDelete(rid,nullptr,nullptr,nullptr);
          } else if (log_record.GetLogRecordType() == LogRecordType::APPLYDELETE) {
            page->ApplyDelete(rid,nullptr,nullptr);
          } else {
            page->RollbackDelete(rid,nullptr,nullptr);
          }
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
        }
      }
      // maintain the active_txn_ table and lsn_mapping_ table along the way.
      auto it = active_txn_.find(log_record.GetTxnId());
      if (it == active_txn_.end())
        active_txn_.emplace(std::make_pair(log_record.GetTxnId(), log_record.GetLSN()));
      else
        it->second = log_record.GetLSN();
      if (log_record.GetLogRecordType() == LogRecordType::COMMIT ||
          log_record.GetLogRecordType() == LogRecordType::ABORT) {
        // this txn is finished
        active_txn_.erase(it);
      }
      lsn_mapping_.emplace(std::make_pair(log_record.GetLSN(), offset_ + pos));
      pos += log_record.GetSize();
    }
    // next disk read start from the last position we fail to deserialize a log record
    offset_ += pos;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  for (const auto& pair:active_txn_) {
    auto lsn = pair.second;
    while (lsn != INVALID_LSN) {
      auto it = lsn_mapping_.find(lsn);
      assert(it != lsn_mapping_.end());
      auto offset = it->second;
      // assume no log record is larger than a page
      disk_manager_->ReadLog(log_buffer_,PAGE_SIZE,offset);
      LogRecord log_record;
      assert(DeserializeLogRecord(log_buffer_,&log_record));
      // complementary operations
      // Insert <-> ApplyDelete, MarkDelete <-> RollBackDelete, Update <-> Update, NewPage <-> (do nothing)
      if (log_record.GetLogRecordType() == LogRecordType::INSERT) {
        auto &rid = log_record.GetInsertRID();
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when undoing insertion");
        page->WLatch();
        page->ApplyDelete(rid,nullptr,nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      } else if (log_record.GetLogRecordType() == LogRecordType::MARKDELETE) {
        auto &rid = log_record.GetInsertRID();
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when undoing insertion");
        page->WLatch();
        page->ApplyDelete(rid,nullptr,nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      } else if (log_record.GetLogRecordType() == LogRecordType::APPLYDELETE) {
        auto &rid = log_record.GetDeleteRID();
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when undoing apply delete");
        page->WLatch();
        page->InsertTuple(log_record.delete_tuple_,&rid,nullptr,nullptr,nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      } else if (log_record.GetLogRecordType() == LogRecordType::ROLLBACKDELETE) {
        auto &rid = log_record.GetDeleteRID();
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when undoing roll back delete");
        page->WLatch();
        page->MarkDelete(rid,nullptr,nullptr,nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      } else if (log_record.GetLogRecordType() == LogRecordType::UPDATE) {
        auto &rid = log_record.update_rid_;
        auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
        if (page == nullptr) throw std::runtime_error("fail to fetch page when undoing update");
        page->WLatch();
        page->UpdateTuple(log_record.old_tuple_,&log_record.new_tuple_,rid,nullptr,nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      }
      lsn = log_record.GetPrevLSN();
    }
  }
}

}  // namespace bustub
