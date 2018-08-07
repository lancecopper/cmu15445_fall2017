/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <math.h>
#include <limits>
#include <cassert>
#include <mutex>
#include <condition_variable>

#include "hash/hash_table.h"

namespace cmudb {
class WfirstRWLock{
public:
  WfirstRWLock() = default;
  ~WfirstRWLock() = default;
public:
  void lock_read();
  void lock_write();
  void release_read();
  void release_write();
private:
  volatile size_t read_cnt{ 0 };
  volatile size_t write_cnt{ 0 };
  volatile bool inwriteflag{ false };
  std::mutex counter_mutex;
  std::condition_variable cond_w;
  std::condition_variable cond_r;
};
template <typename _RWLockable>
class unique_writeguard{
public:
  explicit unique_writeguard(_RWLockable &rw_lockable): rw_lockable_(rw_lockable){
    rw_lockable_.lock_write();
  }
  ~unique_writeguard(){
    rw_lockable_.release_write();
  }
private:
  unique_writeguard() = delete;
  unique_writeguard(const unique_writeguard&) = delete;
  unique_writeguard& operator=(const unique_writeguard&) = delete;
private:
  _RWLockable &rw_lockable_;
};
template <typename _RWLockable>
class unique_readguard{
public:
  explicit unique_readguard(_RWLockable &rw_lockable): rw_lockable_(rw_lockable){
    rw_lockable_.lock_read();
  }
  ~unique_readguard(){
    rw_lockable_.release_read();
  }
private:
  unique_readguard() = delete;
  unique_readguard(const unique_readguard&) = delete;
  unique_readguard& operator=(const unique_readguard&) = delete;
private:
  _RWLockable &rw_lockable_;
};

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  static const int max_global_depth;
public:
  // constructor
  ExtendibleHash(size_t size);
  ~ExtendibleHash();
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  // added by me
  struct bucket{
    int bucket_id;
    int local_depth;
    class WfirstRWLock* local_lk;
    bool* occupied_flag_array;
    K* keys_array;
    V* val_array;
  };
  long long unsigned Hash(const K &key);
  bool SafeInsert(const K &key, const V &value);
  void debug(int mode);
private:
  // add your own member variables here
  size_t bucket_size;
  int global_depth;
  int bucket_num;
  size_t depth_mask;
  struct bucket** directory;
  class WfirstRWLock* global_lk;
  class WfirstRWLock* mask_lk;
};
template <typename K, typename V>
const int ExtendibleHash<K, V>::max_global_depth = 
  round(log10(std::numeric_limits<size_t>::max()) / log10(2));
} // namespace cmudb

