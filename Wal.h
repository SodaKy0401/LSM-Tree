#include <iostream>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>

class Wal {
public:
    std::shared_ptr<std::mutex> file_mutex_;
    std::shared_ptr<std::fstream> file_stream_;

    // 构造函数：使用给定的文件流和互斥量
    explicit Wal(std::shared_ptr<std::mutex> file_mutex, std::shared_ptr<std::fstream> file_stream)
        : file_mutex_(file_mutex), file_stream_(file_stream) {}

    // 创建 WAL 对象，打开指定路径的文件
    static std::shared_ptr<Wal> create(const std::string& path) {
        auto file_stream = std::make_shared<std::fstream>(path, std::ios::in | std::ios::out | std::ios::app);
        if (!file_stream->is_open()) {
            throw std::runtime_error("Failed to open WAL file: " + path);
        }
        auto file_mutex = std::make_shared<std::mutex>();
        return std::make_shared<Wal>(file_mutex, file_stream);
    }

    // 恢复 WAL，暂时没有实现
    static std::shared_ptr<Wal> recover(const std::string& path, const std::unordered_map<std::string, std::string>& skiplist) {
        return create(path); // 恢复过程未实现，仅调用 create 方法
    }

    // 向 WAL 写入键值对，返回 Result 类型
    std::shared_ptr<Wal> put(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
        std::lock_guard<std::mutex> lock(*file_mutex_);
        try {
            std::cout << "Putting key-value pair in WAL: ";
            std::cout << "key: " << std::string(key.begin(), key.end()) << " value: " << std::string(value.begin(), value.end()) << std::endl;
            // 写入逻辑可以添加到这里
            return shared_from_this(); // 如果成功，返回当前对象的 shared pointer
        } catch (const std::exception& e) {
            std::cerr << "Error writing to WAL: " << e.what() << std::endl;
            throw std::runtime_error("Failed to write to WAL");
        }
    }

    // 同步 WAL 文件到磁盘，返回 Result 类型
    std::shared_ptr<Wal> sync() {
        std::lock_guard<std::mutex> lock(*file_mutex_);
        try {
            file_stream_->flush();
            std::cout << "WAL synced to disk" << std::endl;
            return shared_from_this(); // 如果同步成功，返回当前对象的 shared pointer
        } catch (const std::exception& e) {
            std::cerr << "Error syncing WAL to disk: " << e.what() << std::endl;
            throw std::runtime_error("Failed to sync WAL");
        }
    }
};
