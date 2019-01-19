/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 */

#pragma once
#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>

#include "disk/disk_manager.h"
#include "logging/log_record.h"
#include "common/logger.h"

namespace cmudb {

class LogManager {
public:
  LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN),
        disk_manager_(disk_manager) {
    // TODO: you may intialize your own defined memeber variables here
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
    log_buffer_offset_ = 0;
    flush_buffer_offset_ = 0;
    first_flag_ = true;
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }
  // spawn a separate thread to wake up periodically to flush
  void RunFlushThread();
  void StopFlushThread();

  // append a log record into log buffer
  lsn_t AppendLogRecord(LogRecord &log_record);

  // get/set helper functions
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }
  
  // added by lancecopper
  void PeriodFlush();
  void WaitFlush();
  void SwapBuffer();
  void WakeUpFlushThread();

private:
  // TODO: you may add your own member variables
  // also remember to change constructor accordingly

  // atomic counter, record the next log sequence number
  std::atomic<lsn_t> next_lsn_;
  // log records before & include persistent_lsn_ have been written to disk
  std::atomic<lsn_t> persistent_lsn_;
  // log buffer related
  char *log_buffer_;
  char *flush_buffer_;
  int log_buffer_offset_;
  int flush_buffer_offset_;
  // latch to protect shared member variables
  std::mutex latch_;
  // flush thread
  std::thread *flush_thread_;
  // for notifying flush thread
  std::condition_variable cv_;
  // the flush thread will signal for every sucssful flush
  std::condition_variable flushed_cv_;
  // disk manager
  DiskManager *disk_manager_;
  // promise for asynchronized flush coordination
  std::promise<void> *manager_barrier_;
  bool force_flag_;
  std::mutex force_flush_mutex_;
  // added by lancecopper
  lsn_t buffer_max_lsn_;
  bool first_flag_ = true;
};

} // namespace cmudb
