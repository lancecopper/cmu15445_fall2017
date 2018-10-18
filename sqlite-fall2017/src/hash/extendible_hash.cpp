#include <list>
#include <string.h>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

void WfirstRWLock::lock_read(){
  std::unique_lock<std::mutex> ulk(counter_mutex);
  cond_r.wait(ulk, [=]()->bool {return write_cnt == 0; });
  ++read_cnt;
}
void WfirstRWLock::lock_write(){
  std::unique_lock<std::mutex> ulk(counter_mutex);
  ++write_cnt;
  cond_w.wait(ulk, [=]()->bool {return read_cnt == 0 && !inwriteflag; });
  inwriteflag = true;
}
void WfirstRWLock::release_read(){
  std::unique_lock<std::mutex> ulk(counter_mutex);
  if (--read_cnt == 0 && write_cnt > 0){
    cond_w.notify_one();
  }
}
void WfirstRWLock::release_write(){
  std::unique_lock<std::mutex> ulk(counter_mutex);
  if (--write_cnt == 0)  {
      cond_r.notify_all();
  }
  else{
      cond_w.notify_one();
  }
  inwriteflag = false;
}
/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size): bucket_size(size){
  global_depth = 1;
  depth_mask = 1;
  directory = new struct bucket*[2]();
  directory[0] = new bucket { 0, 
                              1,
                              new WfirstRWLock(),
                              new bool[bucket_size](), 
                              new K[bucket_size](), 
                              new V[bucket_size]()
                            };
  directory[1] = new bucket { 1, 
                              1,
                              new WfirstRWLock(),
                              new bool[bucket_size](), 
                              new K[bucket_size](), 
                              new V[bucket_size]()
                            };
  bucket_num = 2;
  global_lk = new WfirstRWLock();
  mask_lk = new WfirstRWLock();
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  /*
  K temp_key = key;
  long long unsigned bitstring = 0;
  unique_readguard<WfirstRWLock> lock(*mask_lk);
  memcpy(&bitstring, &temp_key, sizeof(K));
  bitstring &= depth_mask;
  */
  size_t bitstring;
  if(typeid(key) == typeid(RID)){
    K key_copy = key, *key_p = &key_copy;
    bitstring = std::hash<int64_t>{}(reinterpret_cast<RID *>(key_p)->Get());
  }
  else
    bitstring = std::hash<K>{}(key);
  bitstring &= depth_mask;
  return bitstring;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  unique_readguard<WfirstRWLock> lock(*global_lk);
  return global_depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */          
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  unique_readguard<WfirstRWLock> lock(*global_lk);
  size_t max_subscript = pow(2, global_depth);
  for(size_t offset = 0; offset != max_subscript; offset++){
    if(directory[offset]->bucket_id == bucket_id)
      return directory[offset]->local_depth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  unique_readguard<WfirstRWLock> lock(*global_lk);
  return bucket_num;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  unique_readguard<WfirstRWLock> lock(*global_lk);
  struct bucket* p = directory[HashKey(key)];
  unique_readguard<WfirstRWLock> llock(*(p->local_lk));
  for(size_t offset = 0; offset != bucket_size; offset++){
    if(p->occupied_flag_array[offset] && p->keys_array[offset] == key){
      value = p->val_array[offset];
      return true;
    }
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  unique_readguard<WfirstRWLock> lock(*global_lk);
  struct bucket* p = directory[HashKey(key)];
  unique_writeguard<WfirstRWLock> llock(*(p->local_lk));
  for(size_t offset = 0; offset != bucket_size; offset++){
    if(p->occupied_flag_array[offset] && p->keys_array[offset] == key){
      p->occupied_flag_array[offset] = false;
      return true;
    }
  }
  return false;
}
/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  global_lk->lock_read();
  struct bucket* p = directory[HashKey(key)];
  p->local_lk->lock_write();
  // insert directly if the key or an empty slot is found
  size_t first_empty_offset;
  bool empty_slot_found = false;
  for(size_t offset = 0; offset != bucket_size; offset++){
    if(!empty_slot_found && !p->occupied_flag_array[offset]){
      first_empty_offset = offset;
      empty_slot_found = true;
    }
    if(p->occupied_flag_array[offset] && p->keys_array[offset] == key){
      p->val_array[offset] = value;
      p->local_lk->release_write();
      global_lk->release_read();
      return;
    }
  }
  if(empty_slot_found){
    p->keys_array[first_empty_offset] = key;
    p->val_array[first_empty_offset] = value;
    p->occupied_flag_array[first_empty_offset] = true;
    p->local_lk->release_write();
    global_lk->release_read();
    return;
  }
  p->local_lk->release_write();
  // increase global depth
  global_lk->release_read();
  global_lk->lock_write();
  if(p->local_depth == global_depth){
    assert(global_depth != max_global_depth);
    struct bucket** old_directory = directory;
    size_t directory_size = pow(2, global_depth);
    directory = new struct bucket*[directory_size * 2]();
    memcpy(directory, old_directory, sizeof(struct bucket*) * directory_size);
    memcpy(directory + directory_size, old_directory, sizeof(struct bucket*) * directory_size);
    delete[] old_directory;
    global_depth++;
    mask_lk->lock_write();
    depth_mask = (depth_mask << 1) + 1;
    mask_lk->release_write();
  }
  // Split
  struct bucket* new_bucket0 = new bucket { p->bucket_id, 
                                            p->local_depth + 1,
                                            new WfirstRWLock(),
                                            new bool[bucket_size](), 
                                            new K[bucket_size](), 
                                            new V[bucket_size]()
                                          };
  struct bucket* new_bucket1 = new bucket { bucket_num++, 
                                            p->local_depth + 1,
                                            new WfirstRWLock(), 
                                            new bool[bucket_size](), 
                                            new K[bucket_size](), 
                                            new V[bucket_size]()
                                          };
  size_t max_subscript = pow(2, global_depth);
  size_t i;
  size_t step = pow(2, p->local_depth);
  for(i = 0; i != max_subscript; i++)
    if(directory[i] == p)
      break;
  for(size_t j = 0 ; i < max_subscript; i += step, j++){
    if (j % 2 == 0)
      directory[i] = new_bucket0;
    else
      directory[i] = new_bucket1;
  }
  // redistribute
  for(size_t offset = 0; offset < bucket_size; offset++){
    SafeInsert(p->keys_array[offset], p->val_array[offset]);
  }
  delete[] p->occupied_flag_array;
  delete[] p->keys_array;
  delete[] p->val_array;
  delete p->local_lk;
  delete p;
  // Try to insert again;
  global_lk->release_write();
  Insert(key, value);
  //debug(1);
}
// helper function SafeInsert used in Insert for DRY
template <typename K, typename V>
bool ExtendibleHash<K, V>::SafeInsert(const K &key, const V &value){
  struct bucket* p = directory[HashKey(key)];
  /*
  std::cout << "@@SafeInsert:  " << "key: " << key
            << "HashKey: " << HashKey(key) 
            << "bucket_id" << p->bucket_id << std::endl;
  */
  for(size_t offset = 0; offset != bucket_size; offset++){
    if(!p->occupied_flag_array[offset]){
      p->keys_array[offset] = key;
      p->val_array[offset] = value;
      p->occupied_flag_array[offset] = true;
      return true;
    }
  }
  return false;
}
// destructor
template <typename K, typename V>
ExtendibleHash<K, V>::~ExtendibleHash(){
  struct bucket* p;
  size_t max_subscript = pow(2, global_depth);
  for(size_t i = 0; i != max_subscript; i++){
    if(directory[i] != nullptr){
      p = directory[i];
      size_t step = pow(2, p->local_depth);
      for(size_t j = i; j < max_subscript; j += step)
        directory[j] = nullptr;
      delete[] p->occupied_flag_array;
      delete[] p->keys_array;
      delete[] p->val_array;
      delete p->local_lk;
      delete p;
    }
  }
  delete[] directory;
  delete global_lk;
  delete mask_lk;
}
// help function debug: mode 0 for brief info; mode 1 for detail.
template <typename K, typename V>
void ExtendibleHash<K, V>:: debug(int mode){
  unique_readguard<WfirstRWLock> lock(*global_lk);
  std::cout << "Debug:############################" << std::endl
            << "  global_depth: " << global_depth
            << "  depth_mask" << depth_mask
            << "  bucket_num: "   << bucket_num << std::endl;
  if(mode == 1){
    size_t max_subscript = pow(2, global_depth);
    std::cout << "directory:############################" << std::endl;
    for(size_t i = 0; i != max_subscript; i++){
      struct bucket* p = directory[i];
      std::cout << "  subscript:  " << i
                << "  bucket_id: " << p->bucket_id
                << "  local_depth: " << p->local_depth << std::endl
                << "  bucket_info:##########################" << std::endl;
      for(size_t j = 0; j != bucket_size; j++){
        std::cout << "    offset: " << j
                  << "  occupied?: " << p->occupied_flag_array[j]
                  << "  key: " << p->keys_array[j]
                  << std::endl;
      }
    }
  }
}
template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
template class ExtendibleHash<RID, LockManager::TxnList*>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
