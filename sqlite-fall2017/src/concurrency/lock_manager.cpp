/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

LockManager::LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){
  lock_table_ = new ExtendibleHash<RID, TxnList*>(BUCKET_SIZE);
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // check if the txn is growing(locks shoud not be given to a txn under other condtion)
  if(txn->GetState() != TransactionState::GROWING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Get txn_list and lock on it
  TxnList *txn_list;
  if(!(lock_table_->Find(rid, txn_list))){
    txn_list = new TxnList();
    lock_table_->Insert(rid, txn_list);
  }
  // unlocked or locked in shared mode: insert directly
  txn->GetSharedLockSet()->insert(rid);
  if(txn_list->InsertRead(txn)){
    return true;
  }
  else{
    txn->GetSharedLockSet()->erase(rid);
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if(txn->GetState() != TransactionState::GROWING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  TxnList *txn_list;
  if(!(lock_table_->Find(rid, txn_list))){
    txn_list = new TxnList();
    lock_table_->Insert(rid, txn_list);
  }
  txn->GetExclusiveLockSet()->insert(rid);
  if(txn_list->InsertWrite(txn))
    return true;
  else{
    txn->GetExclusiveLockSet()->erase(rid);
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if(txn->GetState() != TransactionState::GROWING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  TxnList *txn_list;
  // no lock record of rid in hash table, abort.
  if(!(lock_table_->Find(rid, txn_list))){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // txn hasn't  get shared lock on rid, abort.
  LockType ltype;
  if(!(txn_list->Find(txn, ltype)) || ltype != LockType::SHARED){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // try to insert exclusive lock
  if(txn_list->UpgradeFromRToW(txn)){
    return true;
  }
  else{
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if(strict_2PL_ && txn->GetState() != TransactionState::COMMITTED){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if(!strict_2PL_ && txn->GetState() == TransactionState::GROWING)
    txn->SetState(TransactionState::SHRINKING);
  TxnList *txn_list;
  // no lock record of rid in hash table, abort.
  if(!(lock_table_->Find(rid, txn_list))){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if(txn_list->Delete(txn))
    return true;
  else
    return false;
}

bool LockManager::TxnList::Find(Transaction *txn, LockType &ltype){
  std::unique_lock<std::mutex> lck(list_mutex_);
  TxnListNode * node = GetHead();
  while(true){
    if(node == nullptr)
      return false;
    if(node->GetTxnId() == txn->GetTransactionId()){
      ltype = node->GetLType();
      return true;
    }
    node = node->GetNext();
  }
}

bool LockManager::TxnList::InsertRead(Transaction *txn){
  std::unique_lock<std::mutex> lck(list_mutex_);
  TxnListNode * node = GetHead();
  // empty list: insert as head node directly
  if(node == nullptr){
    SetHead(new TxnListNode(txn, LockType::SHARED, nullptr));
    return true;
  }
  bool wait_flag = false;
  while(true){
    // wait or die: when a exclusive lock has existed in the list..
    // die: exclusive node has priorit(smaller transaction id)
    if((node->GetLType() == LockType::WEXCLUSIVE || 
        node->GetLType() == LockType::EXCLUSIVE) &&
        node->GetTxnId() < txn->GetTransactionId())
      return false;
    // wait: exclusive node has bigger transaction id
    if(node->GetLType() == LockType::WEXCLUSIVE || 
        node->GetLType() == LockType::EXCLUSIVE)
      wait_flag = true;
    // add the new node to the end of the linked list
    if(node->GetNext() == nullptr){
      TxnListNode * this_node = new TxnListNode(txn, LockType::SHARED, nullptr);
      node->SetNext(this_node);
      if(wait_flag){
        this_node->SetLType(LockType::WSHARED);
        cond_.wait(lck, [=]()->bool { return txn->GetTransactionId() == waken_txn_id_; });
        // when waken up, it means there is no exclusive node before these node,
        // check whether next node existed and can be waken up too?!
        this_node->SetLType(LockType::SHARED);
        node = this_node->GetNext();
        if(node != nullptr && node->GetLType() == LockType::WSHARED){
          waken_txn_id_ = node->GetTxnId();
          cond_.notify_all();
        }
      }
      return true;
    }
    node = node->GetNext();
  }
}

bool LockManager::TxnList::InsertWrite(Transaction *txn){
  std::unique_lock<std::mutex> lck(list_mutex_);
  TxnListNode * node = GetHead();
  // empty list: insert as head node directly
  if(node == nullptr){
    SetHead(new TxnListNode(txn, LockType::EXCLUSIVE, nullptr));
    return true;
  }
  // any existed node would make this node wait
  while(true){
    // die
    if(node->GetTxnId() < txn->GetTransactionId())
      return false;
    // wait
    if(node->GetNext() == nullptr){
      TxnListNode * this_node = new TxnListNode(txn, LockType::WEXCLUSIVE, nullptr);
      node->SetNext(this_node);
      cond_.wait(lck, [=]()->bool { return txn->GetTransactionId() == waken_txn_id_; });
      this_node->SetLType(LockType::EXCLUSIVE);
      return true;
    }
    node = node->GetNext();
  }
}

// called only by LockManager::LockUpgrade
bool LockManager::TxnList::UpgradeFromRToW(Transaction *txn){
  std::unique_lock<std::mutex> lck(list_mutex_);
  TxnListNode *pre_node = nullptr, *node = GetHead();
  assert(node != nullptr);
  bool wait_flag = false;
  while(true){
    if(node == nullptr){
      TxnListNode *this_node = new TxnListNode(txn, LockType::EXCLUSIVE, nullptr);
      if(pre_node == nullptr)
        SetHead(this_node);
      else
        pre_node->SetNext(this_node);
      // a waken exclusive node must block the next node, so no need to nofiy.
      if(wait_flag){
        this_node->SetLType(LockType::WEXCLUSIVE);
        cond_.wait(lck, [=]()->bool { return txn->GetTransactionId() == waken_txn_id_; });
        this_node->SetLType(LockType::EXCLUSIVE);
      }
      return true;
    }
    // the node is found.
    if(node->GetTxnId() == txn->GetTransactionId()){
      if(pre_node == nullptr)
        SetHead(node->GetNext());
      else
        pre_node->SetNext(node->GetNext());
      node = node->GetNext();
      continue;
    // a failed upgrade may or may not delete the original shared-mode node
    // depending whether the node is deleted before upgrade failure..
    }else if(node->GetTxnId() < txn->GetTransactionId()){
      return false;
    }else
      wait_flag = true;
    pre_node = node;
    node = node->GetNext();
  }
}

bool LockManager::TxnList::Delete(Transaction *txn){
  std::unique_lock<std::mutex> lck(list_mutex_);
  TxnListNode *pre_node = nullptr, *node = GetHead();
  while(true){
    if(node == nullptr)
      return false;
    // the node is found.
    if(node->GetTxnId() == txn->GetTransactionId()){
      if(pre_node == nullptr)
        SetHead(node->GetNext());
      else
        pre_node->SetNext(node->GetNext());
      return true;
    }
    pre_node = node;
    node = node->GetNext();
  }
}

} // namespace cmudb
