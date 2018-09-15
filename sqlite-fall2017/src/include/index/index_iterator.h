/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(const KeyComparator &comparator);

  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *node,
                KeyType key,
                int index,
                BufferPoolManager *buffer_pool_manager,
                const KeyComparator &comparator);
  
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *node,
                BufferPoolManager *buffer_pool_manager,
                const KeyComparator &comparator);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  // add your own private member variables here
  KeyType key_;
  bool active_;
	bool has_key_;
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *node_;
	BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
};

} // namespace cmudb
