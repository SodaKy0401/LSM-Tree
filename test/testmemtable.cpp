#include "../consts.h"
#include "../src/Iterator/iterator.h"
#include "../src/Memtable/memtable.h"
#include <gtest/gtest.h>
#include <string>
#include <utility>
#include <vector>
#include<thread>



// 测试基本的插入和查询操作
TEST(MemTableTest, BasicOperations) {
}

// 测试删除操作
TEST(MemTableTest, RemoveOperations) {
}

// 测试冻结表操作
TEST(MemTableTest, FrozenTableOperations) {
}

// 测试大量数据操作
TEST(MemTableTest, LargeScaleOperations) {
}

// 测试内存大小跟踪
TEST(MemTableTest, MemorySizeTracking) {
}

// 测试多次冻结表操作
TEST(MemTableTest, MultipleFrozenTables) {
}

// 测试迭代器在复杂操作序列下的行为
TEST(MemTableTest, IteratorComplexOperations) {
  MemTable memtable;

  // 第一批操作：基本插入
  memtable.put("key1", "value1", 0);
  memtable.put("key2", "value2", 0);
  memtable.put("key3", "value3", 0);

  // 验证第一批操作
  std::vector<std::pair<std::string, std::string>> result1;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    result1.push_back(*it);
  }
  ASSERT_EQ(result1.size(), 3);
  EXPECT_EQ(result1[0].first, "key1");
  EXPECT_EQ(result1[0].second, "value1");
  EXPECT_EQ(result1[2].second, "value3");

  // 冻结当前表
  memtable.frozen_cur_table();

  // 第二批操作：更新和删除
  memtable.put("key2", "value2_updated", 0); // 更新已存在的key
  memtable.remove("key1", 0);                // 删除一个key
  memtable.put("key4", "value4", 0);         // 插入新key

  // 验证第二批操作
  std::vector<std::pair<std::string, std::string>> result2;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    result2.push_back(*it);
  }
  ASSERT_EQ(result2.size(), 3); // key1被删除，key4被添加
  EXPECT_EQ(result2[0].first, "key2");
  EXPECT_EQ(result2[0].second, "value2_updated");
  EXPECT_EQ(result2[2].first, "key4");

  // 再次冻结当前表
  memtable.frozen_cur_table();

  // 第三批操作：混合操作
  memtable.put("key1", "value1_new", 0); // 重新插入被删除的key
  memtable.remove("key3", 0); // 删除一个在第一个frozen table中的key
  memtable.put("key2", "value2_final", 0); // 再次更新key2
  memtable.put("key5", "value5", 0);       // 插入新key

  // 验证最终结果
  std::vector<std::pair<std::string, std::string>> final_result;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    final_result.push_back(*it);
  }

  // 验证最终状态
  ASSERT_EQ(final_result.size(), 4); // key1, key2, key4, key5

  // 验证具体内容
  EXPECT_EQ(final_result[0].first, "key1");
  EXPECT_EQ(final_result[0].second, "value1_new");

  EXPECT_EQ(final_result[1].first, "key2");
  EXPECT_EQ(final_result[1].second, "value2_final");

  EXPECT_EQ(final_result[2].first, "key4");
  EXPECT_EQ(final_result[2].second, "value4");

  EXPECT_EQ(final_result[3].first, "key5");
  EXPECT_EQ(final_result[3].second, "value5");

  // 验证被删除的key确实不存在
  bool has_key3 = false;
  auto res = memtable.get("key3", 0);
  EXPECT_TRUE(res.get_value().empty());
}


TEST(MemTableTest, IteratorDistinguishMissingAndDeleted) {
  MemTable memtable;

  // 第一批操作：插入一些数据
  memtable.put("key1", "value1", 0); // 插入key1
  memtable.put("key2", "value2", 0); // 插入key2
  memtable.put("key3", "value3", 0); // 插入key3

  // 验证第一批操作
  std::vector<std::pair<std::string, std::string>> result1;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    result1.push_back(*it);
  }
  ASSERT_EQ(result1.size(), 3);
  EXPECT_EQ(result1[0].first, "key1");
  EXPECT_EQ(result1[0].second, "value1");
  EXPECT_EQ(result1[2].first, "key3");
  
  // 冻结当前表
  memtable.frozen_cur_table();

  // 第二批操作：删除一个键，插入另一个键
  memtable.remove("key1", 0); // 删除key1
  memtable.put("key4", "value4", 0); // 插入key4

  // 再次验证操作
  std::vector<std::pair<std::string, std::string>> result2;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    result2.push_back(*it);
  }
  ASSERT_EQ(result2.size(), 3); // key1被删除，key4被添加
  EXPECT_EQ(result2[0].first, "key2");
  EXPECT_EQ(result2[0].second, "value2");
  EXPECT_EQ(result2[2].first, "key4");

  // 再次冻结当前表
  memtable.frozen_cur_table();

  // 第三批操作：混合操作
  memtable.put("key5", "value5", 0); // 插入新key5
  memtable.remove("key3", 0); // 删除key3，这个key已经在第一个冻结表中
  memtable.put("key1", "value1_new", 0); // 重新插入已删除的key1
  
  // 验证最终结果
  std::vector<std::pair<std::string, std::string>> final_result;
  for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
    final_result.push_back(*it);
  }

  // 验证最终状态
  ASSERT_EQ(final_result.size(), 4); // key1, key2, key4, key5

  // 验证具体内容
  EXPECT_EQ(final_result[0].first, "key1");
  EXPECT_EQ(final_result[0].second, "value1_new");

  EXPECT_EQ(final_result[1].first, "key2");
  EXPECT_EQ(final_result[1].second, "value2");

  EXPECT_EQ(final_result[2].first, "key4");
  EXPECT_EQ(final_result[2].second, "value4");

  EXPECT_EQ(final_result[3].first, "key5");
  EXPECT_EQ(final_result[3].second, "value5");

  // 验证删除的key3是否已被标记为删除并且不再返回
  auto res3 = memtable.get("key3", 0);
  EXPECT_TRUE(res3.get_value().empty()); // key3被删除，值为空
  EXPECT_TRUE(res3.is_valid()); // key3应为无效
}

TEST(MemTableTest, GetBatchTest) {
  MemTable memtable;

  // 冻结表之前插入两个 key
  memtable.put("key1", "value1", 1);  // 将来会被删除
  memtable.put("key2", "value2", 1);  
  memtable.frozen_cur_table();       // 冻结为旧版本

  // 活跃表中删除 key1，key2 不存在（即 missing）
  memtable.remove("key1", 1);  // key1 被标记删除
  // key2: 不做任何操作，相当于 missing

  std::vector<std::string> keys = {"key1", "key2", "key3"};  // key3 彻底 missing
  auto results = memtable.get_batch(keys, 2);

  ASSERT_EQ(results.size(), 3);

  // key1 是被删除的（活跃中空，冻结中存在空值）
   EXPECT_EQ(results[0].first, "key1");
   EXPECT_FALSE(results[0].second.has_value()); // nullopt 表示被删除

  // key2 是 missing（活跃中 nullopt，冻结中未出现）
  EXPECT_EQ(results[1].first, "key2");
  EXPECT_EQ(results[0].first, "key1");

  // key3 完全 missing
  EXPECT_EQ(results[2].first, "key3");
  EXPECT_FALSE(results[2].second.has_value());
}



TEST(MemTableTest, ConcurrentOperations) {
}

TEST(MemTableTest, PreffixIter) {
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}