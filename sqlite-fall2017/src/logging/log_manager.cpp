/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include "common/logger.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::WaitFlush(){
  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  flushed_cv_.wait(lk);
}
void LogManager::WakeUpFlushThread(){
  std::unique_lock<std::mutex> lk(latch_);
  SwapBuffer();
  cv_.notify_one();
}
void LogManager::SwapBuffer(){
  if(first_flag_)
    first_flag_ = false;
  else{
    std::future<void> barrier_future = manager_barrier_->get_future();
    barrier_future.wait();
    delete manager_barrier_;
  }
  char *temp_buffer = flush_buffer_;
  flush_buffer_ = log_buffer_;
  log_buffer_ = temp_buffer;
  flush_buffer_offset_ = log_buffer_offset_;
  log_buffer_offset_ = 0;
  buffer_max_lsn_ = next_lsn_ - 1;
  manager_barrier_ = new(std::promise<void>);
}
void LogManager::PeriodFlush() {
  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  std::unique_lock<std::mutex> lk1(latch_);
  lk1.unlock();
  while(ENABLE_LOGGING){
    if(cv_.wait_for(lk, LOG_TIMEOUT, [](){ return false; })){
      // force flush
      ;
    } else{
      // timeout
      lk1.lock();
      SwapBuffer();
      lk1.unlock();
    }
    disk_manager_->WriteLog(flush_buffer_, flush_buffer_offset_); 
    SetPersistentLSN(buffer_max_lsn_);
    memset(flush_buffer_, 0, LOG_BUFFER_SIZE);
    manager_barrier_->set_value();
    flushed_cv_.notify_all();
  }
}
void LogManager::RunFlushThread(){
  ENABLE_LOGGING = true;
  flush_thread_ = new std::thread(&LogManager::PeriodFlush, this);
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  ENABLE_LOGGING = false;
  assert(flush_thread_->joinable());
  flush_thread_->join();
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
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  std::unique_lock<std::mutex> lk(latch_);
  if(log_buffer_offset_ + log_record.size_ > LOG_BUFFER_SIZE){
    SwapBuffer();
    cv_.notify_one();
  }
  // First, serialize the must have fields(20 bytes in total)
  log_record.lsn_ = next_lsn_++;
  memcpy(log_buffer_ + log_buffer_offset_, &log_record, 20);
  int pos = log_buffer_offset_ + 20;
  // OPS specified: insert
  if (log_record.log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
  }
  // OPS specified: delete
  if(log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
     log_record.log_record_type_ == LogRecordType::MARKDELETE ||
     log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  }
  // OPS specified: update
  if (log_record.log_record_type_ == LogRecordType::UPDATE){
    memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += sizeof(int32_t) + log_record.old_tuple_.GetLength();
    log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
  }
  // OPS specified: new page
  if (log_record.log_record_type_ == LogRecordType::NEWPAGE)
    memcpy(log_buffer_ + pos,  &log_record.prev_page_id_, sizeof(page_id_t));
  // update log_buffer_offset_
  log_buffer_offset_ += log_record.size_;
  return log_record.lsn_;
}
} // namespace cmudb
