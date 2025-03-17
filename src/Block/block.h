#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

/***
Refer to https://skyzh.github.io/mini-lsm/week1-03-block.html for memory layout

----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section | Extra |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset
#N| num_of_elements |
----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------

*/

class BlockIterator;

class Block : public std::enable_shared_from_this<Block> {
  friend BlockIterator;

private:
  std::vector<uint8_t> data;
  std::vector<uint16_t> offsets;
  size_t capacity;

  struct Entry {
    std::string key;
    std::string value;
  };
  Entry get_entry_at(size_t offset) const;
  std::string get_key_at(size_t offset) const;
  std::string get_value_at(size_t offset) const;
  int compare_key_at(size_t offset, const std::string &target) const;

public:
  Block() = default;
  Block(size_t capacity);
  // ! 这里的编码函数不包括 hash
  std::vector<uint8_t> encode();
  // ! 这里的解码函数可指定切片是否包括 hash
  static std::shared_ptr<Block> decode(const std::vector<uint8_t> &encoded,
                                       bool with_hash = false);
  std::string get_first_key();
  size_t get_offset_at(size_t idx) const;
  bool add_entry(const std::string &key, const std::string &value);
  std::optional<std::string> get_value_binary(const std::string &key);

  size_t size() const;
  size_t cur_size() const;
  bool is_empty() const;
  std::optional<size_t> get_idx_binary(const std::string &key);

  // 按照谓词返回迭代器, 左闭右开
  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  get_monotony_predicate_iters(std::function<int(const std::string &)> func);

  BlockIterator begin();

  std::optional<
      std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  iters_preffix(const std::string &preffix);

  BlockIterator end();
};