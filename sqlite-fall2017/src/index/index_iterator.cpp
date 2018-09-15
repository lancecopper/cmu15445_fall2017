/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {
/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const KeyComparator &comparator): 
active_(false), comparator_(comparator){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *node,
																	KeyType key,
																	int index,
																	BufferPoolManager *buffer_pool_manager,
																	const KeyComparator &comparator):
key_(key),  active_(true), has_key_(true), index_(index), node_(node), 
buffer_pool_manager_(buffer_pool_manager), comparator_(comparator){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *node,
																	BufferPoolManager *buffer_pool_manager,
																	const KeyComparator &comparator):
active_(true), has_key_(false), index_(0), node_(node), 
buffer_pool_manager_(buffer_pool_manager), comparator_(comparator){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() 
{
	return !active_;
};

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*(){
	assert(active_);
	return node_->GetItem(index_);
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++(){
	//LOG_DEBUG("start..");
	assert(active_);
	int size = node_->GetSize();
	////LOG_DEBUG("iterator++ start: index = %d, size = %d\n", index_, size);
	if(index_ < size - 1)
		index_++;
	else{
		page_id_t next_page_id = node_->GetNextPageId();
		if(next_page_id == INVALID_PAGE_ID){
			active_ = false;
			//LOG_DEBUG("iterator to the end: the last leaf node!");
			buffer_pool_manager_->UnpinPage(node_->GetPageId(), true);
			return *this;
		}
		else{
			auto page = buffer_pool_manager_->FetchPage(next_page_id);
			buffer_pool_manager_->UnpinPage(node_->GetPageId(), true);
			node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
			index_ = 0;	
		}
	}
	/*
	if(has_key_ && comparator_(node_->KeyAt(index_), key_) != 0){
		active_ = false;
		//LOG_DEBUG("iterator to the end: mismatched key!");
	}
	*/
	//LOG_DEBUG("finished!");
	return *this;
};

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
