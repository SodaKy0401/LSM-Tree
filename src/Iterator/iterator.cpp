#include "iterator.h"
#include <vector>

// *************************** SearchItem ***************************
bool operator<(const SearchItem &a, const SearchItem &b) {
  if (a.key != b.key) {
    return a.key < b.key;
  }
  if (a.level < b.level) {
    return true;
  }
  return a.idx < b.idx;
}

bool operator>(const SearchItem &a, const SearchItem &b) {
  if (a.key != b.key) {
    return a.key > b.key;
  }
  if (a.level < b.level) {
    return true;
  }
  return a.idx > b.idx;
}

bool operator==(const SearchItem &a, const SearchItem &b) {
  return a.idx == b.idx && a.key == b.key;
}

// *************************** HeapIterator ***************************
HeapIterator::HeapIterator(std::vector<SearchItem> item_vec) {
  for (auto &item : item_vec) {
    items.push(item);
  }

  while (!items.empty() && items.top().value.empty()) {
    // 如果当前元素的value为空，则说明该元素已经被删除，需要从优先队列中删除
    auto del_key = items.top().key;
    while (!items.empty() && items.top().key == del_key) {
      items.pop();
    }
  }
}

HeapIterator::pointer HeapIterator::operator->() const {
  update_current();
  return current.get();
}

HeapIterator::value_type HeapIterator::operator*() const {
  return std::make_pair(items.top().key, items.top().value);
}

BaseIterator &HeapIterator::operator++() {
  if (items.empty()) {
    return *this; // 处理空队列情况
  }

  auto old_item = items.top();
  items.pop();

  // 删除与旧元素key相同的元素
  while (!items.empty() && items.top().key == old_item.key) {
    items.pop();
  }

  // 处理被删除的元素
  while (!items.empty() && items.top().value.empty()) {
    auto del_key = items.top().key;
    while (!items.empty() && items.top().key == del_key) {
      items.pop();
    }
  }

  return *this;
}

bool HeapIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::HeapIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const HeapIterator &>(other);
  if (items.empty() && other2.items.empty()) {
    return true;
  }
  if (items.empty() || other2.items.empty()) {
    return false;
  }
  return items.top().key == other2.items.top().key &&
         items.top().value == other2.items.top().value;
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

bool HeapIterator::is_end() const { return items.empty(); }
bool HeapIterator::is_valid() const { return !items.empty(); }

void HeapIterator::update_current() const {
  if (!items.empty()) {
    current = std::make_shared<value_type>(items.top().key, items.top().value);
  } else {
    current.reset();
  }
}

IteratorType HeapIterator::get_type() const {
  return IteratorType::HeapIterator;
}