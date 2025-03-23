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
  MemTable memtable;

  // 测试插入和查找
  memtable.put("key1", "value1");
  EXPECT_EQ(memtable.get("key1").value(), "value1");

  // 测试更新
  memtable.put("key1", "new_value");
  EXPECT_EQ(memtable.get("key1").value(), "new_value");

  // 测试不存在的key
  EXPECT_FALSE(memtable.get("nonexistent").has_value());
}

// 测试删除操作
TEST(MemTableTest, RemoveOperations) {
  MemTable memtable;

  // 插入并删除
  memtable.put("key1", "value1");
  memtable.remove("key1");
  EXPECT_TRUE(memtable.get("key1").value().empty());

  // 删除不存在的key
  memtable.remove("nonexistent");
  EXPECT_TRUE(memtable.get("nonexistent").value().empty());
}

// 测试冻结表操作
TEST(MemTableTest, FrozenTableOperations) {
  MemTable memtable;

  // 在当前表中插入数据
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");

  // 冻结当前表
  memtable.frozen_cur_table();

  // 在新的当前表中插入数据
  memtable.put("key3", "value3");

  // 验证所有数据都能被访问到
  EXPECT_EQ(memtable.get("key1").value(), "value1");
  EXPECT_EQ(memtable.get("key2").value(), "value2");
  EXPECT_EQ(memtable.get("key3").value(), "value3");
}

// 测试大量数据操作
TEST(MemTableTest, LargeScaleOperations) {
  MemTable memtable;
  const int num_entries = 1000;

  // 插入大量数据
  for (int i = 0; i < num_entries; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    memtable.put(key, value);
  }

  // 验证数据
  for (int i = 0; i < num_entries; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected = "value" + std::to_string(i);
    EXPECT_EQ(memtable.get(key).value(), expected);
  }
}

// 测试内存大小跟踪
TEST(MemTableTest, MemorySizeTracking) {
  MemTable memtable;

  // 初始大小应该为0
  EXPECT_EQ(memtable.get_total_size(), 0);

  // 添加数据后大小应该增加
  memtable.put("key1", "value1");
  EXPECT_GT(memtable.get_cur_size(), 0);

  // 冻结表后，frozen_size应该增加
  size_t size_before_freeze = memtable.get_total_size();
  memtable.frozen_cur_table();
  EXPECT_EQ(memtable.get_frozen_size(), size_before_freeze);
}

// 测试多次冻结表操作
TEST(MemTableTest, MultipleFrozenTables) {
  MemTable memtable;

  // 第一次冻结
  memtable.put("key1", "value1");
  memtable.frozen_cur_table();

  // 第二次冻结
  memtable.put("key2", "value2");
  memtable.frozen_cur_table();

  // 在当前表中添加数据
  memtable.put("key3", "value3");

  // 验证所有数据都能访问
  EXPECT_EQ(memtable.get("key1").value(), "value1");
  EXPECT_EQ(memtable.get("key2").value(), "value2");
  EXPECT_EQ(memtable.get("key3").value(), "value3");
}

// 测试迭代器在复杂操作序列下的行为
TEST(MemTableTest, IteratorComplexOperations) {
  MemTable memtable;

  // 第一批操作：基本插入
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");
  memtable.put("key3", "value3");

  // 验证第一批操作
  std::vector<std::pair<std::string, std::string>> result1;
  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
    result1.push_back(*it);
  }
  ASSERT_EQ(result1.size(), 3);
  EXPECT_EQ(result1[0].first, "key1");
  EXPECT_EQ(result1[0].second, "value1");
  EXPECT_EQ(result1[2].second, "value3");

  // 冻结当前表
  memtable.frozen_cur_table();

  // 第二批操作：更新和删除
  memtable.put("key2", "value2_updated"); // 更新已存在的key
  memtable.remove("key1");                // 删除一个key
  memtable.put("key4", "value4");         // 插入新key

  // 验证第二批操作
  std::vector<std::pair<std::string, std::string>> result2;
  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
    result2.push_back(*it);
  }
  ASSERT_EQ(result2.size(), 3); // key1被删除，key4被添加
  EXPECT_EQ(result2[0].first, "key2");
  EXPECT_EQ(result2[0].second, "value2_updated");
  EXPECT_EQ(result2[2].first, "key4");

  // 再次冻结当前表
  memtable.frozen_cur_table();

  // 第三批操作：混合操作
  memtable.put("key1", "value1_new"); // 重新插入被删除的key
  memtable.remove("key3"); // 删除一个在第一个frozen table中的key
  memtable.put("key2", "value2_final"); // 再次更新key2
  memtable.put("key5", "value5");       // 插入新key

  // 验证最终结果
  std::vector<std::pair<std::string, std::string>> final_result;
  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
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
  auto res = memtable.get("key3");
  EXPECT_TRUE(res.value().empty());
}

TEST(MemTableTest, ConcurrentOperations) {
  MemTable memtable;
  const int num_readers = 4;       // 读线程数
  const int num_writers = 2;       // 写线程数
  const int num_operations = 1000; // 每个线程的操作数

  // 用于同步所有线程的开始
  std::atomic<bool> start{false};
  // 用于等待所有线程完成
  std::atomic<int> completion_counter{num_readers + num_writers +
                                      1}; // +1 for freeze thread

  // 记录写入的键，用于验证
  std::vector<std::string> inserted_keys;
  std::mutex keys_mutex;

  // 写线程函数
  auto writer_func = [&](int thread_id) {
    while (!start) {
      std::this_thread::yield();
    }

    for (int i = 0; i < num_operations; ++i) {
      std::string key =
          "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
      std::string value =
          "value_" + std::to_string(thread_id) + "_" + std::to_string(i);

      if (i % 3 == 0) {
        // 插入操作
        memtable.put(key, value);
        {
          std::lock_guard<std::mutex> lock(keys_mutex);
          inserted_keys.push_back(key);
        }
      } else if (i % 3 == 1) {
        // 删除操作
        memtable.remove(key);
      } else {
        // 更新操作
        memtable.put(key, value + "_updated");
      }

      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
    }

    completion_counter--;
  };

  // 读线程函数
  auto reader_func = [&](int thread_id) {
    while (!start) {
      std::this_thread::yield();
    }

    int found_count = 0;
    for (int i = 0; i < num_operations; ++i) {
      // 随机选择一个已插入的key进行查询
      std::string key_to_find;
      {
        std::lock_guard<std::mutex> lock(keys_mutex);
        if (!inserted_keys.empty()) {
          key_to_find = inserted_keys[rand() % inserted_keys.size()];
        }
      }

      if (!key_to_find.empty()) {
        auto result = memtable.get(key_to_find);
        if (result.has_value()) {
          found_count++;
        }
      }

      // 每隔一段时间进行一次遍历操作
      if (i % 100 == 0) {
        std::vector<std::pair<std::string, std::string>> items;
        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
          items.push_back(*it);
        }
      }

      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 50));
    }

    completion_counter--;
  };

  // 冻结线程函数
  auto freeze_func = [&]() {
    while (!start) {
      std::this_thread::yield();
    }

    // 定期执行冻结操作
    for (int i = 0; i < 5; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      memtable.frozen_cur_table();

      // 验证冻结后的表
      size_t frozen_size = memtable.get_frozen_size();
      EXPECT_GE(frozen_size, 0);

      // 验证总大小
      size_t total_size = memtable.get_total_size();
      EXPECT_GE(total_size, frozen_size);
    }

    completion_counter--;
  };

  // 创建并启动写线程
  std::vector<std::thread> writers;
  for (int i = 0; i < num_writers; ++i) {
    writers.emplace_back(writer_func, i);
  }

  // 创建并启动读线程
  std::vector<std::thread> readers;
  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(reader_func, i);
  }

  // 创建并启动冻结线程
  std::thread freeze_thread(freeze_func);

  // 给线程一点时间进入等待状态
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 记录开始时间
  auto start_time = std::chrono::high_resolution_clock::now();

  // 发送开始信号
  start = true;

  // 等待所有线程完成
  while (completion_counter > 0) {
    std::this_thread::yield();
  }

  // 记录结束时间
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  // 等待所有线程结束
  for (auto &w : writers) {
    w.join();
  }
  for (auto &r : readers) {
    r.join();
  }
  freeze_thread.join();

  // 验证最终状态
  size_t final_size = 0;
  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
    final_size++;
  }

  // 输出统计信息
  // std::cout << "Concurrent test completed in " << duration.count()
  //           << "ms\nFinal memtable size: " << final_size
  //           << "\nTotal size: " << memtable.get_total_size()
  //           << "\nFrozen size: " << memtable.get_frozen_size() << std::endl;

  // 基本正确性检查
  EXPECT_GT(memtable.get_total_size(), 0);             // 总大小应该大于0
  EXPECT_LE(final_size, num_writers * num_operations); // 大小不应超过最大可能值
}

TEST(MemTableTest, PreffixIter) {
  MemTable memtable;

  // 在当前表中插入数据
  memtable.put("abc", "3");
  memtable.put("abcde", "5");
  memtable.put("abcd", "4");
  memtable.put("xxx", "-1");
  memtable.put("abcdef", "6");
  memtable.put("yyyy", "-1");

  // 冻结当前表
  memtable.frozen_cur_table();

  // 在新的当前表中插入数据
  memtable.put("zz", "-1");
  memtable.put("abcdefg", "7");
  memtable.remove("abcd");
  memtable.put("abcdefgh", "8");
  memtable.put("ab", "2");
  memtable.put("wwwwww", "-1");

  // 冻结当前表
  memtable.frozen_cur_table();

  // 在新的当前表中插入数据
  memtable.put("mmmmm", "-1");
  memtable.remove("ab");
  memtable.put("abc", "33");

  int id = 0;
  std::vector<std::pair<std::string, std::string>> answer{{"abc", "33"},
                                                          {"abcde", "5"},
                                                          {"abcdef", "6"},
                                                          {"abcdefg", "7"},
                                                          {"abcdefgh", "8"}};

  for (auto it = memtable.iters_preffix("ab"); !it.is_end(); ++it) {
    EXPECT_EQ(it->first, answer[id].first);
    EXPECT_EQ(it->second, answer[id].second);
    id++;
  }
}


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}