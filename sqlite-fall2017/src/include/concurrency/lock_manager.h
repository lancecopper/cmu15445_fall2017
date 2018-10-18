/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "hash/extendible_hash.h"

namespace cmudb {

enum class LockType { SHARED = 0, EXCLUSIVE, WSHARED, WEXCLUSIVE };

class LockManager {

public:
  LockManager(bool strict_2PL);
  class TxnList;
  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  bool strict_2PL_;
  // Lance's code below
  HashTable<RID, TxnList*> *lock_table_; // to keep track of locked tuple
};

class LockManager::TxnList{
public:
  TxnList() {};
  class TxnListNode;
  bool Find(Transaction *txn, LockType &ltype);
  bool InsertRead(Transaction *txn);
  bool InsertWrite(Transaction *txn);
  bool UpgradeFromRToW(Transaction *txn);
  bool Delete(Transaction *txn);
  inline std::unique_lock<std::mutex> GetListLock() {
    std::unique_lock<std::mutex> ulk(list_mutex_);
    return std::move(ulk); 
  }
  //inline bool Empty() { return head_ == nullptr; }
  /*
  inline std::unique_lock<std::mutex> GetTupleLock() {
    std::unique_lock<std::mutex> ulk(tuple_mutex_);
    return std::move(ulk); 
  }
  */
  //inline LockType GetLockType() { return ltype_; }
  //inline void SetLockType(LockType ltype) { ltype_ = ltype; }
  //inline size_t GetReadCnt() { return read_cnt_; }
  //inline void IncReadCnt() { read_cnt_++; }
  //inline void DecReadCnt() { read_cnt_--; }
  inline TxnListNode *GetHead() { return head_; }
  inline void SetHead(TxnListNode *head) { head_ = head; }
private:
  std::mutex list_mutex_;
  std::condition_variable cond_;
  volatile txn_id_t waken_txn_id_{ 0 };
  //std::mutex tuple_mutex_;
  //LockType ltype_{ LockType::UNLOCKED };
  //volatile size_t read_cnt_{ 0 };
  //volatile size_t wait_read_cnt_{ 0 };
  //volatile size_t wait_write_cnt_{ 0 };
  TxnListNode *head_{ nullptr };
  // TxnListNode *fist_wait_writer_{ nullptr };
};

class LockManager::TxnList::TxnListNode{
public:
  TxnListNode() {};
  TxnListNode(Transaction* txn, LockType ltype, TxnListNode * next)
    : txn_(txn), ltype_(ltype), next_(next) {};
  inline Transaction *GetTxn() { return txn_; }
  inline void SetTxn(Transaction *txn) { txn_ = txn; }
  inline txn_id_t GetTxnId() { return txn_->GetTransactionId(); }
  inline LockType GetLType() { return ltype_; }
  inline void SetLType(LockType ltype) { ltype_ = ltype; }
  inline TxnListNode *GetNext() { return next_; }
  inline void SetNext(TxnListNode *next) { next_ = next; }
private:
  Transaction *txn_;
  LockType ltype_;
  TxnListNode *next_;
};

} // namespace cmudb
