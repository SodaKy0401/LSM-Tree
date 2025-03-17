#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SstIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
};

class BaseIterator {
public:
  using value_type = std::pair<std::string, std::string>;
  using pointer = value_type *;
  using reference = value_type &;

  virtual BaseIterator &operator++() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual value_type operator*() const = 0;
  virtual IteratorType get_type() const = 0;
  virtual bool is_end() const = 0;
  virtual bool is_valid() const = 0;
};

class SstIterator;
// *************************** SearchItem ***************************
struct SearchItem {
  std::string key;
  std::string value;
  int idx;
  int level; // 来自sst的level

  SearchItem() = default;
  SearchItem(std::string k, std::string v, int i, int l)
      : key(std::move(k)), value(std::move(v)), idx(i), level(l) {}
};

bool operator<(const SearchItem &a, const SearchItem &b);
bool operator>(const SearchItem &a, const SearchItem &b);
bool operator==(const SearchItem &a, const SearchItem &b);

// *************************** HeapIterator ***************************
class HeapIterator : public BaseIterator {
  friend class SstIterator;

public:
  HeapIterator() = default;
  HeapIterator(std::vector<SearchItem> item_vec);
  pointer operator->() const;
  virtual value_type operator*() const override;
  BaseIterator &operator++() override;
  BaseIterator &operator++(int) = delete;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;

  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

private:
  std::priority_queue<SearchItem, std::vector<SearchItem>,
                      std::greater<SearchItem>>
      items;
  mutable std::shared_ptr<value_type> current; // 用于存储当前元素

  void update_current() const;
};