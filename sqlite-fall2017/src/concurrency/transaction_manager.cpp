/**
 * transaction_manager.cpp
 *
 */
#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>
//#include "common/logger.h"
namespace cmudb {

Transaction *TransactionManager::Begin() {
  Transaction *txn = new Transaction(next_txn_id_++);
  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here
    LogRecord log_record(next_txn_id_ - 1, txn->GetPrevLSN(), LogRecordType::BEGIN);
    auto lsn = log_manager_->AppendLogRecord(log_record);
    txn->SetPrevLSN(lsn);
  }
  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);
  // truly delete before commit
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // this also release the lock when holding the page latch
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here
    LOG_DEBUG("transaction commit");
    LogRecord log_record(next_txn_id_ - 1, txn->GetPrevLSN(), LogRecordType::COMMIT);
    auto lsn = log_manager_->AppendLogRecord(log_record);
    txn->SetPrevLSN(lsn);
    LOG_DEBUG("transaction commit2");
    while(lsn - log_manager_->GetPersistentLSN() > 0){
      std::cout << "lsn: " << lsn << "persist_lsn: " << log_manager_->GetPersistentLSN() << std::endl;
      log_manager_->WaitFlush();
    }
    LOG_DEBUG("transaction commit3");
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
  LOG_DEBUG("transaction committed");
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // rollback before releasing lock
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      LOG_DEBUG("rollback delete");
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      LOG_DEBUG("rollback insert");
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      LOG_DEBUG("rollback update");
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here
    LogRecord log_record(next_txn_id_ - 1, txn->GetPrevLSN(), LogRecordType::ABORT);
    auto lsn = log_manager_->AppendLogRecord(log_record);
    txn->SetPrevLSN(lsn);
    while(lsn > log_manager_->GetPersistentLSN())
      log_manager_->WaitFlush();
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}
} // namespace cmudb
