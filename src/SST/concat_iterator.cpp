#include "concat_iterator.h"

ConcactIterator::ConcactIterator(std::vector<std::shared_ptr<SST>> ssts)
    : ssts(ssts), cur_iter(nullptr), cur_idx(0) {
  if (!this->ssts.empty()) {
    cur_iter = ssts[0]->begin();
  }
}

BaseIterator &ConcactIterator::operator++() {
  ++cur_iter;

  if (cur_iter.is_end() || !cur_iter.is_valid()) {
    cur_idx++;
    if (cur_idx < ssts.size()) {
      cur_iter = ssts[cur_idx]->begin();
    } else {
      cur_iter = SstIterator(nullptr);
    }
  }
  return *this;
}

bool ConcactIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::ConcactIterator) {
    return false;
  }
  auto other2 = dynamic_cast<const ConcactIterator &>(other);
  return other2.cur_iter == cur_iter;
}

bool ConcactIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

ConcactIterator::value_type ConcactIterator::operator*() const {
  return *cur_iter;
}

IteratorType ConcactIterator::get_type() const {
  return IteratorType::ConcactIterator;
}

bool ConcactIterator::is_end() const {
  return cur_iter.is_end() || !cur_iter.is_valid();
}

bool ConcactIterator::is_valid() const {
  return !cur_iter.is_end() && cur_iter.is_valid();
}

ConcactIterator::pointer ConcactIterator::operator->() const {
  return cur_iter.operator->();
}

std::string ConcactIterator::key() { return cur_iter.key(); }

std::string ConcactIterator::value() { return cur_iter.value(); }