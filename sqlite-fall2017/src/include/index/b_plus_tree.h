/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "hash/extendible_hash.h"

namespace cmudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
#define MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>
enum class TraverseMode { LEFT = 0, SEARCH, INSERT, DELETE };
// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                      BufferPoolManager *buffer_pool_manager,
                      const KeyComparator &comparator,
                      page_id_t root_page_id = INVALID_PAGE_ID);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // Print this B+ tree to stdout using a simple command-line
  std::string ToString(bool verbose = false);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // expose for test purpose
  Page *FindLeafPage(const KeyType &key,
                     TraverseMode t_mode,
                     bool &root_page_id_locked,
                     Transaction *transaction = nullptr);
  // utility func
  Page *traverse(const KeyType &key, bool left);
  inline Page *PageID2Page(page_id_t page_id){
    auto page = buffer_pool_manager_->FetchPage(page_id);
    if(page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    return page;
  }
  inline BPlusTreePage *PageID2Node(page_id_t page_id){
    auto page = PageID2Page(page_id);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    return node;
  }
  inline void LockRootId(TraverseMode t_mode){
    if(t_mode == TraverseMode::LEFT || t_mode == TraverseMode::SEARCH)
      root_lk->lock_read();
    else if(t_mode == TraverseMode::INSERT || t_mode == TraverseMode::DELETE)
      root_lk->lock_write();
    else
      assert(false);
  }
  inline void UnlockRootId(TraverseMode t_mode){
    if(t_mode == TraverseMode::LEFT || t_mode == TraverseMode::SEARCH)
      root_lk->release_read();
    else if(t_mode == TraverseMode::INSERT || t_mode == TraverseMode::DELETE)
      root_lk->release_write();
    else
      assert(false);
  }
  inline bool IsSafe(BPlusTreePage *node, TraverseMode t_mode){
    if(t_mode == TraverseMode::INSERT)
      return node->GetSize() < node->GetMaxSize();
    else if(t_mode == TraverseMode::DELETE)
      return node->GetSize() > node->GetMinSize();
    else
      assert(false);
  }
  inline void LockPage(Page* page, TraverseMode t_mode){
    if(t_mode == TraverseMode::LEFT || t_mode == TraverseMode::SEARCH)
      page->RLatch();
    else if(t_mode == TraverseMode::INSERT || t_mode == TraverseMode::DELETE)
      page->WLatch();
    else
      assert(false);
  }
  inline void UnLockPage(Page* page, TraverseMode t_mode){
    if(t_mode == TraverseMode::LEFT || t_mode == TraverseMode::SEARCH)
      page->RUnlatch();
    else if(t_mode == TraverseMode::INSERT || t_mode == TraverseMode::DELETE)
      page->WUnlatch();
    else
      assert(false);
  }
  inline void UnLockTxnPage(Transaction *transaction, 
                            TraverseMode t_mode,
                            bool &root_page_id_locked, 
                            bool dirty = false)
  {
    LOG_DEBUG("start..");
    assert(t_mode == TraverseMode::INSERT || t_mode == TraverseMode::DELETE);
    std::shared_ptr<std::deque<Page *>> page_set = transaction->GetPageSet();
    /*
    for(auto iter = page_set->cbegin(); iter != page_set->cend(); iter++){
      std::cout << (*iter)->GetPageId() << " " << std::endl;
    }
    std::cout << std::endl;
    */
    if(root_page_id_locked){
      UnlockRootId(t_mode);
      root_page_id_locked = false;
    }
    while(!page_set->empty()){
      auto page = page_set->front();
      page_set->pop_front();
      //std::cout << page->GetPageId() << std::endl;
      UnLockPage(page, t_mode);
      assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), dirty));
    }
    LOG_DEBUG("start..");
  }
  inline void DeletePage(Transaction *transaction, page_id_t page_id){
    LOG_DEBUG("start..");
    std::cout << "page_id" << page_id << std::endl;
    std::shared_ptr<std::deque<Page *>> page_set = transaction->GetPageSet();
    bool find_flag = false;
    for(auto iter = page_set->cbegin(); iter != page_set->cend(); iter++){
      if((*iter)->GetPageId() == page_id){
        page_set->erase(iter);
        find_flag = true;
        UnLockPage(*iter, TraverseMode::DELETE);
        break;
      }
    }
    assert(find_flag);
    transaction->AddIntoDeletedPageSet(page_id);        
    assert(buffer_pool_manager_->UnpinPage(page_id, true));    
    assert(buffer_pool_manager_->DeletePage(page_id));
  }

private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(
      N *&neighbor_node, N *&node,
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
      int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  class WfirstRWLock* root_lk;
};

} // namespace cmudb
