#include "../LSM-Tree/src/LSM/engine.h"
#include "src/Compact/two_merge_iterator.h"
#include <iostream>
#include <string>

int main() {
  // 创建LSM实例，指定数据存储目录
  LSM lsm("./data");

  // 插入数据
  lsm.put("key1", "value1");
  lsm.put("key2", "value2");
  lsm.put("key3", "value3");

  // 查询数据
  auto value1 = lsm.get("key1");
  if (value1.has_value()) {
    std::cout << "key1: " << value1.value() << std::endl;
  } else {
    std::cout << "key1 not found" << std::endl;
  }

  // 更新数据
  lsm.put("key1", "new_value1");
  auto new_value1 = lsm.get("key1");
  if (new_value1.has_value()) {
    std::cout << "key1: " << new_value1.value() << std::endl;
  } else {
    std::cout << "key1 not found" << std::endl;
  }

  // 删除数据
  lsm.remove("key2");
  auto value2 = lsm.get("key2");
  if (value2.has_value()) {
    std::cout << "key2: " << value2.value() << std::endl;
  } else {
    std::cout << "key2 not found" << std::endl;
  }

  // 遍历所有数据
  std::cout << "All key-value pairs:" << std::endl;
  for (auto it = lsm.begin(); it != lsm.end(); ++it) {
    std::cout << it->first << ": " << it->second << std::endl;
  }

  return 0;
}