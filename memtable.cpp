#include <iostream>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <optional>
#include <mutex>
#include <functional>

// 自定义哈希函数，用于 std::unordered_map
struct VectorHash {
    template <typename T>
    std::size_t operator ()(const std::vector<T>& v) const {
        std::size_t hash = 0;
        for (const auto& el : v) {
            hash ^= std::hash<T>{}(el) + 0x9e3779b9 + (hash << 6) + (hash >> 2);  // 经典的哈希合并方法
        }
        return hash;
    }
};

// 模拟写前日志（WAL）的同步功能
class WAL {
public:
    void sync() {
        // 实际实现代码
        std::cout << "WAL synchronized.\n";
    }
};

// MemTable 类的定义
class MemTable {
public:
    MemTable(size_t id) : id(id), approximate_size(0) {}

    // 插入键值对
    void put(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
        size_t estimated_size = key.size() + value.size();
        map[key] = value; // 插入到 map 中
        approximate_size.fetch_add(estimated_size, std::memory_order_relaxed);
    }

    // 同步 WAL
    void sync_wal() {
        if (wal) {
            wal->sync();
        }
    }

    // 获取范围内的迭代器
    // 注意这里简化了迭代器的实现
    void scan(const std::optional<std::vector<uint8_t>>& lower,
              const std::optional<std::vector<uint8_t>>& upper) {
        for (const auto& entry : map) {
            const auto& key = entry.first;
            if (lower && key < *lower) continue;
            if (upper && key > *upper) continue;

            // 打印符合条件的键值对
            std::cout << "Key: ";
            for (auto byte : key) {
                std::cout << (int)byte << " ";
            }
            std::cout << "\nValue: ";
            for (auto byte : entry.second) {
                std::cout << (int)byte << " ";
            }
            std::cout << std::endl;
        }
    }

    // 获取 MemTable 的 id
    size_t get_id() const {
        return id;
    }

    // 获取 MemTable 的近似大小
    size_t approximate_size_value() const {
        return approximate_size.load(std::memory_order_relaxed);
    }

    // 设置 WAL（写前日志）
    void set_wal(std::shared_ptr<WAL> wal_ptr) {
        wal = wal_ptr;
    }

private:
    size_t id;
    std::atomic<size_t> approximate_size;
    std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>, VectorHash> map; // 使用自定义哈希函数
    std::shared_ptr<WAL> wal;  // Write-Ahead Log (WAL)
};

// 迭代器类，简化了实际实现
class MemTableIterator {
public:
    MemTableIterator(std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>, VectorHash>::iterator begin,
                     std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>, VectorHash>::iterator end)
        : current(begin), end(end) {}

    bool is_valid() const {
        return current != end;
    }

    const std::vector<uint8_t>& key() const {
        return current->first;
    }

    const std::vector<uint8_t>& value() const {
        return current->second;
    }

    void next() {
        if (current != end) {
            ++current;
        }
    }

private:
    std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>, VectorHash>::iterator current;
    std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>, VectorHash>::iterator end;
};

// 用于测试的 main 函数
int main() {
    MemTable mem_table(1);

    // 模拟插入键值对
    std::vector<uint8_t> key1 = {1, 2, 3};
    std::vector<uint8_t> value1 = {4, 5, 6};
    mem_table.put(key1, value1);

    std::vector<uint8_t> key2 = {7, 8, 9};
    std::vector<uint8_t> value2 = {10, 11, 12};
    mem_table.put(key2, value2);

    // 设置 WAL
    auto wal = std::make_shared<WAL>();
    mem_table.set_wal(wal);

    // 测试同步 WAL
    mem_table.sync_wal();

    // 测试扫描方法
    std::optional<std::vector<uint8_t>> lower = std::make_optional(std::vector<uint8_t>{1});
    std::optional<std::vector<uint8_t>> upper = std::make_optional(std::vector<uint8_t>{8});
    mem_table.scan(lower, upper);

    // 打印 MemTable 的信息
    std::cout << "MemTable ID: " << mem_table.get_id() << std::endl;
    std::cout << "MemTable approximate size: " << mem_table.approximate_size_value() << std::endl;

    return 0;
}
