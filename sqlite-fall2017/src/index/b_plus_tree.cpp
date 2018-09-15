/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

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
  //LOG_DEBUG("start");
  if(IsEmpty())
    return false;
  ValueType value;
  auto leaf_node = traverse(key);
  if(leaf_node->Lookup(key, value, comparator_)){
    result.push_back(value);
    assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false));
    return true;
  }
  assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false));
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
  //LOG_DEBUG("start..");
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
  //LOG_DEBUG("start");
  page_id_t page_id;
  Page *page;
  if((page = (buffer_pool_manager_->NewPage(page_id))) == nullptr)
    throw Exception("out of memory");
  root_page_id_ = page_id;
  UpdateRootPageId(true);
  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  root->Init(page_id, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
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
  //LOG_DEBUG("start..");
  ValueType temp_value;
  auto leaf_node = traverse(key);
  // if the key has existed in the leaf, return false.
  if(leaf_node->Lookup(key, temp_value, comparator_)){
    assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false));
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
    assert(buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true));
  }
  assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true));
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
  //LOG_DEBUG("start..");
  page_id_t page_id;
  Page *page;
  if((page = (buffer_pool_manager_->NewPage(page_id))) == nullptr)
    throw Exception("out of memory");
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
  //LOG_DEBUG("start");
  // deal with depth increase
  if(old_node->IsRootPage()){
    page_id_t page_id;
    Page *page;
    if((page = (buffer_pool_manager_->NewPage(page_id))) == nullptr)
      throw Exception("out of memory");
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *root = 
      reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
    root_page_id_ = page_id;
    UpdateRootPageId(false);
    root->Init(page_id, INVALID_PAGE_ID);
    old_node->SetParentPageId(page_id);
    new_node->SetParentPageId(page_id);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
    //LOG_DEBUG("depth increase finished");
    return;
  }
  // find the parent page
  auto parent_id = old_node->GetParentPageId();
  auto parent = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                  (PageID2Node(parent_id));
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // deal with recursive split  
  if(parent->GetSize() > parent->GetMaxSize())
  {
    //LOG_DEBUG("recursive split");
    MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_internal_node = Split(parent);
    KeyType split_key = new_internal_node->KeyAt(0);
    InsertIntoParent(static_cast<BPlusTreePage *>(parent), 
                     split_key, 
                     static_cast<BPlusTreePage *>(new_internal_node), 
                     transaction);
    assert(buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true));;
  }
  assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
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
  //LOG_DEBUG("start..");
  if(IsEmpty())
    return;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = traverse(key);
  // deal with redistributte or merge
  if(leaf_node->RemoveAndDeleteRecord(key, comparator_) < leaf_node->GetMinSize())
    CoalesceOrRedistribute(leaf_node, transaction);
  else
    assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true));
  //LOG_DEBUG("end..");
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
  //LOG_DEBUG("start..");
  if(node->IsRootPage())
    return AdjustRoot(node);
  page_id_t parent_id, left_sib_id, right_sib_id;  
  parent_id = node->GetParentPageId();
  auto parent = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                  (PageID2Node(parent_id));
  int index = parent->ValueIndex(node->GetPageId());
  N *left_sib, *right_sib;
  // try to redistribute with left sibling
  if(index != 0){
    left_sib_id = parent->ValueAt(index - 1);
    left_sib = reinterpret_cast<N *>(PageID2Node(left_sib_id));
    if(left_sib->GetSize() + node->GetSize() > node->GetMaxSize()){
      //LOG_DEBUG("redistribute with left sibling");
      Redistribute(left_sib, node, index);
      assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
      assert(buffer_pool_manager_->UnpinPage(left_sib_id, true));
      assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
      return false;
    }
  }
  // redistribute with right sibling
  if(index != parent->GetSize() - 1){
    right_sib_id = parent->ValueAt(index + 1);
    right_sib = reinterpret_cast<N *>(PageID2Node(right_sib_id));
    if(right_sib->GetSize() + node->GetSize() > node->GetMaxSize()){
      //LOG_DEBUG("redistribute with right sibling");
      Redistribute(right_sib, node, 0);
      assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
      assert(buffer_pool_manager_->UnpinPage(right_sib_id, true));
      assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
      if(index != 0)
        assert(buffer_pool_manager_->UnpinPage(left_sib_id, true));
      return false;
    }
  }
  // coalesce
  if(index == 0){
    //LOG_DEBUG("coalesce 1");
    Coalesce(right_sib, node, parent, 0, transaction);
    return false;
  }
  //LOG_DEBUG("coalesce 2");
  if(index != parent->GetSize() - 1)
    assert(buffer_pool_manager_->UnpinPage(right_sib_id, false));
  Coalesce(left_sib, node, parent, index, transaction);
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
  //LOG_DEBUG("start..");
  if(index == 0){
    neighbor_node->MoveAllTo(node, 1, buffer_pool_manager_);
    parent->Remove(parent->ValueIndex(neighbor_node->GetPageId()));
    assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true));
    assert(buffer_pool_manager_->DeletePage(neighbor_node->GetPageId()));
  }else{
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(parent->ValueIndex(node->GetPageId()));
    assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), true));
    assert(buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true));
    assert(buffer_pool_manager_->DeletePage(node->GetPageId()));
  }
  // deal with redistributte or merge
  if(parent->GetSize() < parent->GetMinSize()){
    if(CoalesceOrRedistribute(parent, transaction))
      return true;
    else
      return false;
  }
  assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
  return false;
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
  //LOG_DEBUG("start..");
  page_id_t parent_id = node->GetParentPageId();
  auto parent = reinterpret_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>
                  (PageID2Node(parent_id));
  if(index == 0){
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  }else{
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  assert(buffer_pool_manager_->UnpinPage(parent_id, true));
  //LOG_DEBUG("end..");
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
  //LOG_DEBUG("start..");
  // case 1
  if(old_root_node->GetSize() == 1 && !(old_root_node->IsLeafPage())){
    root_page_id_ = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(old_root_node)
                    ->RemoveAndReturnOnlyChild();
    UpdateRootPageId(false);
    assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
    assert(buffer_pool_manager_->DeletePage(old_root_node->GetPageId()));
    return true;
  }
  if(old_root_node->GetSize() == 0 && old_root_node->IsLeafPage()){
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
    assert(buffer_pool_manager_->DeletePage(old_root_node->GetPageId()));
    return true;
  }
  assert(buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true));
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() 
{ 
  KeyType *key = reinterpret_cast<KeyType *>(malloc(sizeof(KeyType)));
  B_PLUS_TREE_LEAF_PAGE_TYPE *node;
  if((node = FindLeafPage(*key, true)) == nullptr){
    free(key);
    return INDEXITERATOR_TYPE(comparator_);
  }
  free(key);
  return INDEXITERATOR_TYPE(node, buffer_pool_manager_, comparator_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *node;
  if((node = FindLeafPage(key, false)) == nullptr)
    return INDEXITERATOR_TYPE(comparator_); 
  return INDEXITERATOR_TYPE(node, key, 
                            node->KeyIndex(key, comparator_), 
                            buffer_pool_manager_, 
                            comparator_);
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
                                                         bool leftMost) 
{
  if(IsEmpty())
    return nullptr; 
  ValueType value;
  // your code  here
  auto leaf_node = traverse(key, leftMost);
  if(leftMost)
    return leaf_node;
  else{
    if(leaf_node->Lookup(key, value, comparator_))
      return leaf_node;
    assert(buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false));
    return nullptr;
  }
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
  if(header_page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
  if(root_page_id_ == INVALID_PAGE_ID)
    header_page->DeleteRecord(index_name_);
  else if(insert_record)
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
std::string BPLUSTREE_TYPE::ToString(bool verbose) { 
  //LOG_DEBUG("start..");
  if(IsEmpty())
    return "Empty tree";
  std::ostringstream ret_os;
  std::list<BPlusTreePage *> a, b, *new_nodes_list = &a, *old_nodes_list = &b, *temp_nodes_list;
  int depth = 0;
  auto node = PageID2Node(root_page_id_);
  old_nodes_list->push_back(node);
  while(!(old_nodes_list->front()->IsLeafPage())){
    ret_os << "depth: " << depth++ << " ####################" << std::endl;
    // std::cout << "depth: " << depth << " ####################" << std::endl;
    while(!(old_nodes_list->empty())){
      node = old_nodes_list->front();
      old_nodes_list->pop_front();
      int i = 0, size = node->GetSize();
      ret_os << static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node)->ToString(verbose) << std::endl;
      //std::cout << static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node)->ToString(verbose) << std::endl;
      //LOG_DEBUG("pnt1");
      for(; i < size - 1; i++){
        page_id_t page_id = static_cast<MY_B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(node)->ValueAt(i);
        auto child_node = PageID2Node(page_id);
        new_nodes_list->push_back(child_node);
      }
      assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), false));
      //LOG_DEBUG("pnt2");
    }
    temp_nodes_list = old_nodes_list;
    old_nodes_list = new_nodes_list;
    new_nodes_list = temp_nodes_list;
  }    
  ret_os << "depth: " << depth++ << " ####################" << std::endl;
  //std::cout << "depth: " << depth << " ####################" << std::endl;
  while(!(old_nodes_list->empty())){
    node = old_nodes_list->front();
    old_nodes_list->pop_front();
    ret_os << static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node)->ToString(verbose) << std::endl;
    //std::cout << static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node)->ToString(verbose) << std::endl;
    assert(buffer_pool_manager_->UnpinPage(node->GetPageId(), false));
  }
  return ret_os.str();
}

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