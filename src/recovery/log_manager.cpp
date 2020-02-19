//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

#include <include/common/logger.h>

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
  bustub::enable_logging = true;
  thread_created = true;
  flush_thread_ = new std::thread(&LogManager::Flush, this);
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  bustub::enable_logging = false;
  flush_thread_->join();
}

void LogManager::Flush() {
  while (bustub::enable_logging) {
    std::unique_lock<std::mutex> lck(cv_mutex_);
    cv_.wait_for(lck, bustub::log_timeout, [this] { return flush_start_; });
    // if the flush buffer is empty, swap flush buffer and log buffer
    // else flush the remaining content is flush buffer
    if (flush_buffer_len_ == 0 && log_buffer_len_ != 0) SwapBuffer();
    // flush the log
    bool need_notify_caller = flush_start_;
    if (flush_buffer_len_ != 0) {
      LOG_INFO("Flushing to disk\n");
      disk_manager_->WriteLog(flush_buffer_, flush_buffer_len_);
      flush_buffer_len_ = 0;
      if (need_notify_caller) flush_finished_ = true;
      persistent_lsn_.store(last_lsn_in_flush_buffer_);
    }
    lck.unlock();
    if (need_notify_caller) {
      cv_.notify_one();
    }
  }
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  if (log_buffer_len_ + log_record->GetSize() > LOG_BUFFER_SIZE) {
    // log buffer remaining space can not contain the log record
    if (flush_buffer_len_ != 0) {
      // if the flush buffer is not emtpy, manually trigger the flush
      TriggerFlush();
    }
    // now the flush buffer is empty, swap two buffers
    SwapBuffer();
  }
  // write the log record to the log buffer
  log_record->lsn_ = next_lsn_++;
  // copy the common header part for the record
  memcpy(log_buffer_ + log_buffer_len_, log_record, 20);
  int pos = log_buffer_len_ + 20;
  assert(log_record->log_record_type_ != LogRecordType::INVALID);
  // copy the remaining part to the buffer according to its type
  if (log_record->log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
    memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record->old_tuple_.GetLength();
    log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
    memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
  } else if (log_record->log_record_type_ != LogRecordType::BEGIN &&
             log_record->log_record_type_ != LogRecordType::COMMIT &&
             log_record->log_record_type_ != LogRecordType::ABORT) {
    // deletion
    memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
  }
  log_buffer_len_ += log_record->GetSize();
  return log_record->lsn_;
}

void LogManager::TriggerFlush() {
  {
    std::lock_guard<std::mutex> guard(cv_mutex_);
    flush_start_ = true;
  }
  cv_.notify_one();
  {
    // wait for the flush thread to finish
    std::unique_lock<std::mutex> lck(cv_mutex_);
    cv_.wait(lck, [this] { return flush_finished_; });
    // reset predicates
    flush_start_ = false;
    flush_finished_ = false;
  }
}

void LogManager::SwapBuffer() {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  assert(flush_buffer_len_ == 0);
  // swap the log buffer and the flush buffer if the flush buffer is empty
  flush_buffer_len_.store(log_buffer_len_.load());
  log_buffer_len_ = 0;
  std::swap(flush_buffer_, log_buffer_);
  if (next_lsn_ > 1) last_lsn_in_flush_buffer_ = next_lsn_ - 1;
}

}  // namespace bustub
