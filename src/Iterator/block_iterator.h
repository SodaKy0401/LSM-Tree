#pragma once
#include "../Block/block.h"
#include "iterator.h"
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>

class Block;

class BlockIterator {
public:
  // 标准迭代器类型定义
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::pair<std::string, std::string>;
  using difference_type = std::ptrdiff_t;
  using pointer = const value_type *;
  using reference = const value_type &;

  // 构造函数
  BlockIterator(std::shared_ptr<Block> b, size_t index);
  BlockIterator(std::shared_ptr<Block> b, const std::string &key);
  BlockIterator(std::shared_ptr<Block> b);
  BlockIterator() : block(nullptr), current_index(0) {} // end iterator

  // 迭代器操作
  pointer operator->() const;
  BlockIterator &operator++();
  BlockIterator operator++(int) = delete;
  bool operator==(const BlockIterator &other) const;
  bool operator!=(const BlockIterator &other) const;
  value_type operator*() const;
  bool is_end();

private:
  std::shared_ptr<Block> block;                   // 指向所属的 Block
  size_t current_index;                           // 当前位置的索引
  mutable std::optional<value_type> cached_value; // 缓存当前值

  void update_current() const;
};