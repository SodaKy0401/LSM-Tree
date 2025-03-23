#pragma once

#include "../Iterator/iterator.h"
#include "../SST/sst_iterator.h"

#include <memory>

class TwoMergeIterator : public BaseIterator {
private:
  std::shared_ptr<BaseIterator> it_a;
  std::shared_ptr<BaseIterator> it_b;
  bool choose_a = false;
  mutable std::shared_ptr<value_type> current; // 用于存储当前元素

  void update_current() const;

public:
  TwoMergeIterator();
  TwoMergeIterator(std::shared_ptr<BaseIterator> it_a, std::shared_ptr<BaseIterator> it_b);
  bool choose_it_a();
  void skip_it_b();

  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual value_type operator*() const override;
  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

  pointer operator->() const;
};