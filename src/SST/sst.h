#pragma once

#include "../Block/block.h"
#include "../Block/block_cache.h"
#include "../Block/blockmeta.h"
#include "../Utils/bloom_filter.h"
#include "../Utils/files.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

class SstIterator;


class SST : public std::enable_shared_from_this<SST> {
  friend class SSTBuilder;
  friend std::optional<std::pair<SstIterator, SstIterator>>
  sst_iters_monotony_predicate(
      std::shared_ptr<SST> sst, uint64_t tranc_id,
      std::function<int(const std::string &)> predicate);

private:
  FileObj file;
  std::vector<BlockMeta> meta_entries;
  uint32_t bloom_offset;
  uint32_t meta_block_offset;
  size_t sst_id;
  std::string first_key;
  std::string last_key;
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<BlockCache> block_cache;
  uint64_t min_tranc_id_ = UINT64_MAX;
  uint64_t max_tranc_id_ = 0;

public:
  // 从文件中打开sst
  static std::shared_ptr<SST> open(size_t sst_id, FileObj file,
                                   std::shared_ptr<BlockCache> block_cache);
  void del_sst();
  // 创建一个sst, 只包含首尾key的元数据
  static std::shared_ptr<SST> create_sst_with_meta_only(
      size_t sst_id, size_t file_size, const std::string &first_key,
      const std::string &last_key, std::shared_ptr<BlockCache> block_cache);
  // 根据索引读取block
  std::shared_ptr<Block> read_block(size_t block_idx);

  // 找到key所在的block的idx
  size_t find_block_idx(const std::string &key);

  // 根据key返回迭代器
  SstIterator get(const std::string &key, uint64_t tranc_id);

  // 返回sst中block的数量
  size_t num_blocks() const;

  // 返回sst的首key
  std::string get_first_key() const;

  // 返回sst的尾key
  std::string get_last_key() const;

  // 返回sst的大小
  size_t sst_size() const;

  // 返回sst的id
  size_t get_sst_id() const;

  std::optional<std::pair<SstIterator, SstIterator>>
  iters_monotony_predicate(std::function<bool(const std::string &)> predicate);

  SstIterator begin(uint64_t tranc_id);
  SstIterator end();

  std::pair<uint64_t, uint64_t> get_tranc_id_range() const;
};

class SSTBuilder {
private:
  Block block;//前正在构建的Block
  std::string first_key;
  std::string last_key;
  std::vector<BlockMeta> meta_entries;
  std::vector<uint8_t> data;
  size_t block_size;
  std::shared_ptr<BloomFilter> bloom_filter;
  uint64_t min_tranc_id_ = UINT64_MAX;
  uint64_t max_tranc_id_ = 0;

public:
  SSTBuilder(size_t block_size, bool has_bloom); // 创建一个sst构建器, 指定目标block的大小
  void add(const std::string &key, const std::string &value, uint64_t tranc_id);// 添加一个key-value对
  // 估计sst的大小
  size_t estimated_size() const;
  // 完成当前block的构建, 即将block写入data, 并创建新的block
  void finish_block();
  // 构建sst, 将sst写入文件并返回SST描述类
  std::shared_ptr<SST> build(size_t sst_id, const std::string &path,
                             std::shared_ptr<BlockCache> block_cache);
};