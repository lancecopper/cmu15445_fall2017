/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  deque_ = new std::deque<T>;
  lk_ = new WfirstRWLock();
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
  delete deque_;
  delete lk_;
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  unique_writeguard<WfirstRWLock> lk(*lk_);
  auto it = deque_->begin();
  while(it != deque_->end())
    if(*it++ == value){
      deque_->erase(--it);
      break;
    }
  deque_->push_back(value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  unique_writeguard<WfirstRWLock> lk(*lk_);
  size_t deque_size = deque_->size();
  if(deque_size != 0){
    value = *(deque_->begin());
    deque_->pop_front();
    return true;
  }
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  unique_writeguard<WfirstRWLock> lk(*lk_);
  auto it = deque_->begin();
  while(it != deque_->end())
    if(*it++ == value){
      deque_->erase(--it);
      return true;
    }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  unique_readguard<WfirstRWLock> lk(*lk_);
  size_t deque_size = deque_->size();
  return deque_size; 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
