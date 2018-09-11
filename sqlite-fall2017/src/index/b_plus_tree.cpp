/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

#define MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) 
{
  ValueType value;  
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!(node->IsLeafPage())){
    RID rid = (static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node))->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = 
                        static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
  if(leaf_node->Lookup(key, value, comparator_)){
    result.push_back(value);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) 
{
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) 
{
  page_id_t page_id;
  if((buffer_pool_manager_->NewPage(page_id)) == nullptr)
    throw Exception("out of memory");
  root_page_id_ = page_id;
  UpdateRootPageId(true);
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  root->Init(page_id, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) 
{
  ValueType temp_value;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!(node->IsLeafPage())){
    RID rid= (static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node))->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
  // if the key has existed in the leaf, return false.
  if(leaf_node->Lookup(key, temp_value, comparator_)){
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  // deal with split
  if(leaf_node->Insert(key, value, comparator_) > leaf_node->GetMaxSize()){
    B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_node = Split(leaf_node);
    KeyType split_key = new_leaf_node->KeyAt(0);
    InsertIntoParent(static_cast<BPlusTreePage *>(leaf_node), 
                     split_key, 
                     static_cast<BPlusTreePage *>(new_leaf_node), 
                     transaction);
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) 
{ 
  page_id_t page_id;
  if((buffer_pool_manager_->NewPage(page_id)) == nullptr)
    throw Exception("out of memory");
  auto page = buffer_pool_manager_->FetchPage(page_id);
  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id, node->GetParentPageId());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node; 
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
  // deal with depth increase
  if(old_node->IsRootPage()){
    page_id_t page_id;
    if((buffer_pool_manager_->NewPage(page_id)) == nullptr)
      throw Exception("out of memory");
    root_page_id_ = page_id;
    UpdateRootPageId(false);
    auto page = buffer_pool_manager_->FetchPage(root_page_id_);
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *root = 
      reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
    root->Init(page_id, INVALID_PAGE_ID);
    old_node->SetPageId(page_id);
    new_node->SetPageId(page_id);
    if(old_node->IsLeafPage()){
      root->InsertNodeAfter(INVALID_PAGE_ID, 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(old_node)->KeyAt(0), 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(old_node)->GetPageId());
      root->InsertNodeAfter(old_node->GetPageId(), 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->KeyAt(0), 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->GetPageId());
    }else{
      root->InsertNodeAfter(INVALID_PAGE_ID, 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(old_node)->KeyAt(0), 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(old_node)->GetPageId());
      root->InsertNodeAfter(old_node->GetPageId(), 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(new_node)->KeyAt(0), 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(new_node)->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  // find the parent page
  page_id_t parent_id = old_node->GetParentPageId();  
  auto page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  if(old_node->IsLeafPage())
    parent->InsertNodeAfter(old_node->GetPageId(), 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->KeyAt(0), 
                            static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(new_node)->GetPageId());
  else
    parent->InsertNodeAfter(old_node->GetPageId(), 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(new_node)->KeyAt(0), 
                            static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(new_node)->GetPageId());
  // deal with recursive split  
  if(parent->GetSize() > parent->GetMaxSize())
  {
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_internal_node = Split(parent);
    KeyType split_key = new_internal_node->KeyAt(0);
    InsertIntoParent(static_cast<BPlusTreePage *>(parent), 
                     split_key, 
                     static_cast<BPlusTreePage *>(new_internal_node), 
                     transaction);
    buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty())
    return;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!(node->IsLeafPage())){
    RID rid = (static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node))->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = 
                        static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
  // deal with redistributte or merge
  if(leaf_node->RemoveAndDeleteRecord(key, comparator_) < leaf_node->GetMinSize())
    CoalesceOrRedistribute(leaf_node, transaction);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) 
{
  if(node->IsRootPage())
    return AdjustRoot(node);
  page_id_t parent_id = node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  N *left_sib, *right_sib;
  // try to redistribute with left sibling
  if(index != 0){
    page = buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1));
    left_sib = reinterpret_cast<N *>(page);
    if(left_sib->GetSize() + node->GetSize() > node->GetMaxSize()){
      Redistribute(left_sib, node, index);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(left_sib->GetPageId(), true);
      return false;
    }
  }
  // redistribute with right sibling
  if(index != parent->GetSize()){
    page = buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1));
    right_sib = reinterpret_cast<N *>(page);
    if(right_sib->GetSize() + node->GetSize() > node->GetMaxSize()){
      Redistribute(right_sib, node, 0);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(right_sib->GetPageId(), true);
      return false;
    }
  }
  // coalesce
  if(index == 0){
    Coalesce(right_sib, node, parent, 0, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_sib->GetPageId(), true);
    return false;
  }
  Coalesce(left_sib, node, parent, index, transaction);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(left_sib->GetPageId(), true);
  if(index != parent->GetSize())
    buffer_pool_manager_->UnpinPage(right_sib->GetPageId(), false);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) 
{
  if(index == 0){
    neighbor_node->MoveAllTo(node, 1, buffer_pool_manager_);
    parent->Remove(parent->ValueIndex(neighbor_node->GetPageId()));
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  }else{
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(parent->ValueIndex(node->GetPageId()));
    buffer_pool_manager_->DeletePage(node->GetPageId());
  // deal with redistributte or merge
  if(parent->GetSize() < parent->GetMinSize())
    return CoalesceOrRedistribute(parent, transaction);
  return false;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) 
{
  page_id_t parent_id = node->GetParentPageId();
  auto page = buffer_pool_manager_->FetchPage(parent_id);
  MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = 
    reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  if(index == 0){
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_)
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  };
  else{
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 1
  // there is some problem here, RemoveAndReturnOnlyChild shoud return a page_id_t, not RID
  if(old_root_node->GetSize() == 1 && !(old_root_node->IsLeafPage())){
    root_page_id_ = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(old_root_node)
                    ->RemoveAndReturnOnlyChild();
    UpdateRootPageId(false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  if(old_root_node->GetSize() == 0 && old_root_node->IsLeafPage()){
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
