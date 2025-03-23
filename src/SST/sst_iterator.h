#pragma once
#include "../Iterator/block_iterator.h"
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

class SstIterator;
class SST;

std::optional<std::pair<SstIterator, SstIterator>>
sst_iters_monotony_predicate(std::shared_ptr<SST> sst,
                             std::function<int(const std::string &)> predicate);

class SstIterator : public BaseIterator {
  friend std::optional<std::pair<SstIterator, SstIterator>>
  sst_iters_monotony_predicate(
      std::shared_ptr<SST> sst,
      std::function<int(const std::string &)> predicate);

  friend SST;

private:
  std::shared_ptr<SST> m_sst;
  size_t m_block_idx;
  std::shared_ptr<BlockIterator> m_block_it;
  mutable std::optional<value_type> cached_value; // 缓存当前值

  void update_current() const;
  void set_block_idx(size_t idx);
  void set_block_it(std::shared_ptr<BlockIterator> it);

public:
  // 创建迭代器, 并移动到第一个key
  SstIterator(std::shared_ptr<SST> sst);
  // 创建迭代器, 并移动到第指定key
  SstIterator(std::shared_ptr<SST> sst, const std::string &key);

  // 创建迭代器, 并移动到第指定前缀的首端或者尾端
  static std::optional<std::pair<SstIterator, SstIterator>>
  iters_monotony_predicate(std::shared_ptr<SST> sst,
                           std::function<bool(const std::string &)> predicate);

  void seek_first();
  void seek(const std::string &key);
  std::string key();
  std::string value();

  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual value_type operator*() const override;
  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

  pointer operator->() const;

  static std::pair<HeapIterator, HeapIterator>
  merge_sst_iterator(std::vector<SstIterator> iter_vec);
};