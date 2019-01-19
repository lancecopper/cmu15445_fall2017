/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"
#include "common/logger.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord &log_record) {
  // First, deserialize the must have fields(20 bytes in total)
  // Ensure at least 20 bytes, otherwise we'll get incomplete record
  int pos = 0;
  if(data + 20 > log_buffer_ + LOG_BUFFER_SIZE){
    return false;
  }
  memcpy(&log_record, data, 20);
  pos += 20;
  // incomplete record
  if(data + log_record.size_ > log_buffer_ + LOG_BUFFER_SIZE){
    return false;
  } 
  // OPS specified: beging, commit, abort
  else if (log_record.log_record_type_ == LogRecordType::BEGIN ||
      log_record.log_record_type_ == LogRecordType::COMMIT ||
      log_record.log_record_type_ == LogRecordType::ABORT) {
    ;
  }
  // OPS specified: insert
  else if (log_record.log_record_type_ == LogRecordType::INSERT) {
    memcpy(&log_record.insert_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    // we have provided serialize function for tuple class
    log_record.insert_tuple_.DeserializeFrom(data + pos);
  }
  // OPS specified: delete
  else if(log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
     log_record.log_record_type_ == LogRecordType::MARKDELETE ||
     log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(&log_record.delete_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.DeserializeFrom(data + pos);
  }
  // OPS specified: update
  else if (log_record.log_record_type_ == LogRecordType::UPDATE){
    memcpy(&log_record.update_rid_, data + pos, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.DeserializeFrom(data + pos);
    pos += sizeof(int32_t) + log_record.old_tuple_.GetLength();
    log_record.new_tuple_.DeserializeFrom(data + pos);
  }
  // OPS specified: new page
  else if (log_record.log_record_type_ == LogRecordType::NEWPAGE)
    memcpy(&log_record.prev_page_id_, data + pos, sizeof(page_id_t));
  // update log_buffer_offset_
  else{
    return false;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  int offset = 0;
  while(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset)){
    LogRecord log_record;
    int pos = 0;
    while(DeserializeLogRecord(log_buffer_ + pos, log_record)){
      // real redo work
      if(log_record.log_record_type_ ==  LogRecordType::INSERT){
        auto page_id = log_record.insert_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->InsertTuple(log_record.insert_tuple_, log_record.insert_rid_, 
                            nullptr, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::APPLYDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::MARKDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::ROLLBACKDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::UPDATE){
        auto page_id = log_record.update_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->UpdateTuple(log_record.new_tuple_, log_record.old_tuple_, log_record.update_rid_, 
                            nullptr, nullptr, nullptr); 
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      // to do: I have some problems here, fetchpage may fetch page with different page_id 
      // for the same newpage log, factually, what I do below is to redo no matter whether
      // the change has been persistent. So following logs may operate on wrong pages.
      // the solution is add a field in NEWPAGE log record recording page_id;
      if(log_record.log_record_type_ ==  LogRecordType::NEWPAGE){
        auto page_id = log_record.prev_page_id_;
        if(page_id == INVALID_PAGE_ID){
          auto first_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
          first_page->Init(page_id, PAGE_SIZE, INVALID_LSN, nullptr, nullptr);
          assert(buffer_pool_manager_->UnpinPage(page_id, true));
        } else{
          auto prev_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
          page_id = prev_page->GetNextPageId();
          if(page_id == INVALID_PAGE_ID){
            auto page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
            prev_page->SetNextPageId(page_id);
            page->Init(page_id, PAGE_SIZE, page->GetPageId(), nullptr, nullptr);
            assert(buffer_pool_manager_->UnpinPage(page_id, true));
          } else{
          // may be, I should use new page instead of fetchpage here!  
            auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
            if(page->GetLSN() < log_record.lsn_)
              page->Init(page_id, PAGE_SIZE, page->GetPageId(), nullptr, nullptr);
            assert(buffer_pool_manager_->UnpinPage(page_id, true));
          }
          assert(buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true));
        }
      }
      // build active_txn_
      if(log_record.log_record_type_ == LogRecordType::BEGIN)
        active_txn_.emplace(log_record.txn_id_, log_record.lsn_);
      else if(log_record.log_record_type_ == LogRecordType::COMMIT || 
         log_record.log_record_type_ == LogRecordType::ABORT)
        active_txn_.erase(log_record.txn_id_);
      else{
        assert(active_txn_.find(log_record.txn_id_) != active_txn_.end());
        active_txn_[log_record.txn_id_] = log_record.lsn_;
      }
      // build lsn_mapping_ table
      lsn_mapping_[log_record.lsn_] = offset + pos;
      pos += log_record.size_;
    }
    offset += pos;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  while(!(active_txn_.empty())){
    // find the latest one of all txn's last log
    auto latest_txn = active_txn_.end();
    for(auto iter = active_txn_.begin(); iter != active_txn_.end(); iter++){
      if(latest_txn == active_txn_.end() || latest_txn->second < iter->second)
        latest_txn = iter;
    }
    // construct log record
    int offset = lsn_mapping_[latest_txn->second];
    disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset);
    LogRecord log_record;
    DeserializeLogRecord(log_buffer_, log_record);
    // deal with the real work
    if(log_record.log_record_type_ ==  LogRecordType::INSERT){
        auto page_id = log_record.insert_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->ApplyDelete (log_record.insert_rid_, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::APPLYDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->InsertTuple(log_record.delete_tuple_,log_record.delete_rid_, nullptr, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::MARKDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->RollbackDelete (log_record.delete_rid_, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::ROLLBACKDELETE){
        auto page_id = log_record.delete_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
      if(log_record.log_record_type_ ==  LogRecordType::UPDATE){
        auto page_id = log_record.update_rid_.GetPageId();
        auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if(page->GetLSN() < log_record.lsn_)
          page->UpdateTuple(log_record.old_tuple_, log_record.new_tuple_, log_record.update_rid_, 
                            nullptr, nullptr, nullptr); 
        assert(buffer_pool_manager_->UnpinPage(page_id, true));
      }
    // update prev lsn for the selected txn
    if(log_record.prev_lsn_ == INVALID_LSN)
      active_txn_.erase(log_record.txn_id_);
    else
      active_txn_[log_record.txn_id_] = log_record.prev_lsn_;
  }
}

} // namespace cmudb
