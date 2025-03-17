#include "skiplist.h"
#include <iostream>
#include <stdexcept>

// ************************ SkipListIterator ************************

BaseIterator &SkipListIterator::operator++() {
  if (current) {
    current = current->forward[0];
  }
  return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
  if (other.get_type() != IteratorType::SkipListIterator)
    return false;
  auto other2 = dynamic_cast<const SkipListIterator &>(other);
  return current == other2.current;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
  return !(*this == other);
}

SkipListIterator::value_type SkipListIterator::operator*() const {
  if (!current)
    throw std::runtime_error("Dereferencing invalid iterator");
  return {current->key, current->value};
}

IteratorType SkipListIterator::get_type() const {
  return IteratorType::SkipListIterator;
}

bool SkipListIterator::is_valid() const { return !current->value.empty(); }
bool SkipListIterator::is_end() const { return current == nullptr; }

std::string SkipListIterator::get_key() const { return current->key; }
std::string SkipListIterator::get_value() const { return current->value; }

// ************************ SkipList ************************
// 构造函数
SkipList::SkipList(int max_lvl) : max_level(max_lvl), current_level(1) {
  head = std::make_shared<SkipListNode>("", "", max_level);
  dis_01 = std::uniform_int_distribution<>(0, 1);
  dis_level = std::uniform_int_distribution<>(0, (1 << max_lvl) - 1);
  gen = std::mt19937(std::random_device()());
}

int SkipList::random_level() {
  int level = 1;
  // 通过"抛硬币"的方式随机生成层数：
  // - 每次有50%的概率增加一层
  // - 确保层数分布为：第1层100%，第2层50%，第3层25%，以此类推
  // - 层数范围限制在[1, max_level]之间，避免浪费内存
  while (dis_01(gen) && level < max_level) {
    level++;
  }
  return level;
}

// 插入或更新键值对
void SkipList::put(const std::string &key, const std::string &value) {
  std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);

  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  auto current = head;

  // 从最高层开始查找插入位置
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  // 移动到最底层
  current = current->forward[0];

  // 如果key已存在，更新value
  if (current && current->key == key) {
    size_bytes += value.size() - current->value.size();
    current->value = value;
    return;
  }

  // 如果key不存在，创建新节点
  int new_level = std::max(random_level(), current_level);
  if (new_level > current_level) {
    for (int i = current_level; i < new_level; ++i) {
      update[i] = head;
    }
  }

  // 生成一个随机数，用于决定是否在每一层更新节点
  int random_bits = dis_level(gen);

  auto new_node = std::make_shared<SkipListNode>(key, value, new_level);
  size_bytes += key.size() + value.size();

  // 更新各层的指针
  for (int i = 0; i < new_level; ++i) {
    bool need_update = false;
    if (i == 0 || (new_level > current_level) || (random_bits & (1 << i))) {
      // 按照如下顺序判断是否进行更新
      // 1. 第0层总是更新
      // 2. 如果需要创建新的层级, 这个节点需要再之前所有的层级上都更新
      // 3. 否则, 根据随机数的位数按照50%的概率更新
      need_update = true;
    }

    if (need_update) {
      new_node->forward[i] = update[i]->forward[i]; // 可能为nullptr
      if (new_node->forward[i]) {
        new_node->forward[i]->set_backward(i, new_node);
      }
      update[i]->forward[i] = new_node;
      new_node->set_backward(i, update[i]);
    } else {
      // 如果不更新当前层，之后更高的层级都不更新
      break;
    }
  }

  current_level = new_level;
}

// 查找键值对
std::optional<std::string> SkipList::get(const std::string &key) {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);

  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key < key) {
      current = current->forward[i];
    }
  }
  // 移动到最底层
  current = current->forward[0];
  // 如果找到匹配的key，返回value
  if (current && current->key == key) {
    return current->value;
  }
  // 未找到返回空
  return std::nullopt;
}

// 删除键值对
// ! 这里的 remove 是跳表本身真实的 remove,  lsm 应该使用 put 空值表示删除,
// ! 这里只是为了实现完整的 SkipList 不会真正被上层调用
void SkipList::remove(const std::string &key) {
  std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);

  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  auto current = head;

  // 从最高层开始查找目标节点
  for (int i = current->forward.size() - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  // 移动到最底层
  current = current->forward[0];

  // 如果找到目标节点，执行删除操作
  if (current && current->key == key) {
    // 更新每一层的 forward 指针，跳过目标节点
    for (int i = 0; i < current_level; ++i) {
      if (update[i]->forward[i] != current) {
        break;
      }
      update[i]->forward[i] = current->forward[i];
    }

    // 更新 backward 指针
    for (int i = 0; i < current->backward.size() && i < current_level; ++i) {
      if (current->forward[i]) {
        current->forward[i]->set_backward(i, update[i]);
      }
    }

    // 更新跳表的内存大小
    size_bytes -= key.size() + current->value.size();

    // 如果删除的节点是最高层的节点，更新跳表的当前层级
    while (current_level > 1 && head->forward[current_level - 1] == nullptr) {
      current_level--;
    }
  }
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::pair<std::string, std::string>> SkipList::flush() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);

  std::vector<std::pair<std::string, std::string>> data;
  auto node = head->forward[0];
  while (node) {
    data.emplace_back(node->key, node->value);
    node = node->forward[0];
  }
  return data;
}

size_t SkipList::get_size() {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  return size_bytes;
}

// 清空跳表，释放内存
void SkipList::clear() {
  // std::unique_lock<std::shared_mutex> lock(rw_mutex);
  head = std::make_shared<SkipListNode>("", "", max_level);
  size_bytes = 0;
}

SkipListIterator SkipList::begin() {
  // return SkipListIterator(head->forward[0], rw_mutex);
  return SkipListIterator(head->forward[0]);
}

SkipListIterator SkipList::end() {
  return SkipListIterator(); // 使用空构造函数
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
  // std::shared_lock<std::shared_mutex> slock(rw_mutex);

  auto current = head;
  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key < preffix) {
      current = current->forward[i];
    }
  }
  // 移动到最底层
  current = current->forward[0];

  return SkipListIterator(current);
}

// 找到前缀的终结位置
SkipListIterator SkipList::end_preffix(const std::string &prefix) {
  auto current = head;

  // 从最高层开始查找
  for (int i = current_level - 1; i >= 0; --i) {
    while (current->forward[i] && current->forward[i]->key < prefix) {
      current = current->forward[i];
    }
  }

  // 移动到最底层
  current = current->forward[0];

  // 找到第一个键不以给定前缀开头的节点
  while (current && current->key.substr(0, prefix.size()) == prefix) {
    current = current->forward[0];
  }

  // 返回当前节点的迭代器
  return SkipListIterator(current);
}

// 返回第一个满足谓词的位置和最后一个满足谓词的迭代器
// 如果不存在, 范围nullptr
// 谓词作用于key, 且保证满足谓词的结果只在一段连续的区间内, 例如前缀匹配的谓词
// predicate返回值:
//   0: 谓词
//   >0: 不满足谓词, 需要向右移动
//   <0: 不满足谓词, 需要向左移动
std::optional<std::pair<SkipListIterator, SkipListIterator>>
SkipList::iters_monotony_predicate(
    std::function<int(const std::string &)> predicate) {
  auto current = head;
  SkipListIterator begin_iter = SkipListIterator(nullptr);
  SkipListIterator end_iter = SkipListIterator(nullptr);

  // 从最高层开始查找
  // 一开始 current == head, 所以  current_level - 1 处肯定有合法的指针
  bool find1 = false;
  for (int i = current_level - 1; i >= 0; --i) {
    while (!find1) {
      auto forward_i = current->forward[i];
      if (forward_i == nullptr) {
        break;
      }
      auto direction = predicate(forward_i->key);
      if (direction == 0) {
        // current 已经满足谓词了
        find1 = true;
        current = forward_i;
        break;
      } else if (direction < 0) {
        // 下一个位置不满足谓词, 且方向错误(位于目标区间右侧)
        // 需要尝试更小的步长(层级)
        break;
      } else {
        // 下一个位置不满足谓词, 但方向正确(位于目标区间左侧)
        current = forward_i;
      }
    }
  }

  if (!find1) {
    // 无法找到第一个满足谓词的迭代器, 直接返回
    return std::nullopt;
  }

  // 记住当前 current 的位置
  auto current2 = current;

  // current 已经满足谓词, 但有可能中途跳过了节点, 需要前向检查
  // 注意此时 不能直接从 current_level - 1 层开始,
  // 因为当前节点的层数不一定等于最大层数
  for (int i = current->backward.size() - 1; i >= 0; --i) {
    while (true) {
      if (current->backward[i].lock() == nullptr ||
          current->backward[i].lock() == head) {
        // 当前层没有前向节点, 或前向节点指向头结点
        break;
      }
      auto direction = predicate(current->backward[i].lock()->key);
      if (direction == 0) {
        // 前一个位置满足谓词, 继续判断
        current = current->backward[i].lock();
        continue;
      } else if (direction > 0) {
        // 前一个位置不满足谓词
        // 需要尝试更小的步长(层级)
        break;
      } else {
        // 因为当前位置满足了谓词, 前一个位置不可能返回-1
        // 这种情况属于跳表实现错误, 需要排查
        throw std::runtime_error("iters_predicate: invalid direction");
      }
    }
  }

  // 找到第一个满足谓词的节点
  begin_iter = SkipListIterator(current);

  // 找到最后一个满足谓词的节点
  for (int i = current2->forward.size() - 1; i >= 0; --i) {
    while (true) {
      if (current2->forward[i] == nullptr) {
        // 当前层没有后向节点
        break;
      }
      auto direction = predicate(current2->forward[i]->key);
      if (direction == 0) {
        // 后一个位置满足谓词, 继续判断
        current2 = current2->forward[i];
        continue;
      } else if (direction < 0) {
        // 后一个位置不满足谓词
        // 需要尝试更小的步长(层级)
        break;
      } else {
        // 因为当前位置满足了谓词, 后一个位置不可能返回1
        // 这种情况属于跳表实现错误, 需要排查
        throw std::runtime_error("iters_predicate: invalid direction");
      }
    }
  }

  end_iter = SkipListIterator(current2);
  // 转化为开区间
  ++end_iter;

  return std::make_optional<std::pair<SkipListIterator, SkipListIterator>>(
      begin_iter, end_iter);
}

void SkipList::print_skiplist() {
  for (int level = 0; level < current_level; level++) {
    std::cout << "Level " << level << ": ";
    auto current = head->forward[level];
    while (current) {
      std::cout << current->key;
      current = current->forward[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
}