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

/**
 * SST文件的结构, 参考自 https://skyzh.github.io/mini-lsm/week1-04-sst.html
 * ------------------------------------------------------------------------
 * |         Block Section         |  Meta Section | Extra                |
 * ------------------------------------------------------------------------
 * | data block | ... | data block |    metadata   | metadata offset (32) |
 * ------------------------------------------------------------------------

 * 其中, metadata 是一个数组加上一些描述信息, 数组每个元素由一个 BlockMeta
 编码形成 MetaEntry, MetaEntry 结构如下:
 * ---------------------------------------------------------------------------------------------------
 * | offset(32) | 1st_key_len(16) | 1st_key(1st_key_len) | last_key_len(16) |
 last_key(last_key_len) |
 * ---------------------------------------------------------------------------------------------------

 * Meta Section 的结构如下:
 * ---------------------------------------------------------------
 * | num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
 * ---------------------------------------------------------------
 * 其中, num_entries 表示 metadata 数组的长度, Hash 是 metadata
 数组的哈希值(只包括数组部分, 不包括 num_entries ), 用于校验 metadata 的完整性
 */

class SST : public std::enable_shared_from_this<SST> {
  friend class SSTBuilder;
  friend std::optional<std::pair<SstIterator, SstIterator>>
  sst_iters_monotony_predicate(
      std::shared_ptr<SST> sst,
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
  SstIterator get(const std::string &key);

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

  // TODO: 单调谓词查询 (这里的单调谓词主要指 范围查询 和 前缀查询)
  std::optional<std::pair<SstIterator, SstIterator>>
  iters_monotony_predicate(std::function<bool(const std::string &)> predicate);

  SstIterator begin();
  SstIterator end();
};

class SSTBuilder {
private:
  Block block;
  std::string first_key;
  std::string last_key;
  std::vector<BlockMeta> meta_entries;
  std::vector<uint8_t> data;
  size_t block_size;
  std::shared_ptr<BloomFilter> bloom_filter;

public:
  // 创建一个sst构建器, 指定目标block的大小
  SSTBuilder(size_t block_size, bool has_bloom); // 添加一个key-value对
  void add(const std::string &key, const std::string &value);
  // 估计sst的大小
  size_t estimated_size() const;
  // 完成当前block的构建, 即将block写入data, 并创建新的block
  void finish_block();
  // 构建sst, 将sst写入文件并返回SST描述类
  std::shared_ptr<SST> build(size_t sst_id, const std::string &path,
                             std::shared_ptr<BlockCache> block_cache);
};