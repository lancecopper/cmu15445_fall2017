/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  deque_ = new std::deque<T>;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
  delete deque_;
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  Erase(value);
  deque_->push_back(value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  size_t deque_size = Size();
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
  auto it = deque_->begin();
  while(it != deque_->end())
    if(*it++ == value){
      deque_->erase(--it);
      return true;
    }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  size_t deque_size = deque_->size();
  return deque_size; 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
