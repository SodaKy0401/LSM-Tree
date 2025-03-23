#pragma once

#include "../Memtable/memtable.h"
#include "../SST/sst.h"
#include "../Compact/compact.h"
#include "../Compact/two_merge_iterator.h"
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class LSMEngine {
public:
  std::string data_dir;
  MemTable memtable;
  std::unordered_map<size_t, std::deque<size_t>> level_sst_ids;
  std::unordered_map<size_t, std::shared_ptr<SST>> ssts;
  std::shared_mutex ssts_mtx;
  std::shared_ptr<BlockCache> block_cache;
  size_t cur_max_sst_id = 0;
  size_t cur_max_level = 0;

public:
  LSMEngine(std::string path);
  ~LSMEngine();

  std::optional<std::string> get(const std::string &key);

  void put(const std::string &key, const std::string &value);
  void put_batch(const std::vector<std::pair<std::string, std::string>> &kvs);
  void remove(const std::string &key);
  void remove_batch(const std::vector<std::string> &keys);
  void clear();
  void flush();
  void flush_all();

  std::string get_sst_path(size_t sst_id, size_t target_level);

  std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
  lsm_iters_monotony_predicate(
      std::function<int(const std::string &)> predicate);

  TwoMergeIterator begin();
  TwoMergeIterator end();

  static size_t get_sst_size(size_t level);

private:
  void full_compact(size_t src_level);
  std::vector<std::shared_ptr<SST>>
  full_l0_l1_compact(std::vector<size_t> &l0_ids, std::vector<size_t> &l1_ids);

  std::vector<std::shared_ptr<SST>>
  full_common_compact(std::vector<size_t> &lx_ids, std::vector<size_t> &ly_ids,
                      size_t level_y);

  std::vector<std::shared_ptr<SST>> gen_sst_from_iter(BaseIterator &iter,
                                                      size_t target_sst_size,
                                                      size_t target_level);
};

class LSM {
private:
  LSMEngine engine;

public:
  LSM(std::string path);
  ~LSM();

  std::optional<std::string> get(const std::string &key);

  void put(const std::string &key, const std::string &value);
  void put_batch(const std::vector<std::pair<std::string, std::string>> &kvs);

  void remove(const std::string &key);
  void remove_batch(const std::vector<std::string> &keys);

  using LSMIterator = TwoMergeIterator;
  LSMIterator begin();
  LSMIterator end();
  std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
  lsm_iters_monotony_predicate(
      std::function<int(const std::string &)> predicate);
  void clear();
  void flush();
  void flush_all();
};