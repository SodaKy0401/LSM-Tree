#include <map>
#include <vector>
#include <string>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <cassert>

using Bytes = std::string;

// MemTable 结构体
class MemTable {
public:
    explicit MemTable(size_t id) : id_(id), approximate_size_(0) {}

    std::optional<Bytes> get(const Bytes& key) {
        std::shared_lock lock(mutex_);
        auto it = map_.find(key);
        return it != map_.end() ? std::optional(it->second) : std::nullopt;
    }

    void put(const Bytes& key, const Bytes& value) {
        std::unique_lock lock(mutex_);
        map_[key] = value;
        approximate_size_ += key.size() + value.size();
    }

    size_t approximate_size() const {
        return approximate_size_;
    }

private:
    size_t id_;
    std::map<Bytes, Bytes> map_;
    size_t approximate_size_;
    mutable std::shared_mutex mutex_;
};

// LsmStorageState 结构体
struct LsmStorageState {
    std::shared_ptr<MemTable> memtable;
    std::vector<std::shared_ptr<MemTable>> imm_memtables;
    std::vector<size_t> l0_sstables;

    explicit LsmStorageState(size_t memtable_id) {
        memtable = std::make_shared<MemTable>(memtable_id);
    }
};

// LsmStorageOptions 结构体
struct LsmStorageOptions {
    size_t block_size = 4096;
    size_t target_sst_size = 2 << 20;
    size_t num_memtable_limit = 50;
    bool enable_wal = false;
};

// LsmStorageInner 结构体
class LsmStorageInner {
public:
    explicit LsmStorageInner(const LsmStorageOptions& options)
        : options_(options), next_sst_id_(1), state_(std::make_shared<LsmStorageState>(next_sst_id_)) {}

    std::optional<Bytes> get(const Bytes& key) {
        std::shared_lock lock(state_mutex_);
        auto snapshot = state_;

        if (auto val = snapshot->memtable->get(key)) return val;
        for (const auto& memtable : snapshot->imm_memtables) {
            if (auto val = memtable->get(key)) return val;
        }
        return std::nullopt;
    }

    void put(const Bytes& key, const Bytes& value) {
        assert(!key.empty() && !value.empty());
        {
            std::shared_lock lock(state_mutex_);
            state_->memtable->put(key, value);
        }
        try_freeze();
    }

    void try_freeze() {
        if (state_->memtable->approximate_size() >= options_.target_sst_size) {
            std::unique_lock lock(state_mutex_);
            if (state_->memtable->approximate_size() >= options_.target_sst_size) {
                force_freeze_memtable();
            }
        }
    }

    void force_freeze_memtable() {
        auto new_memtable = std::make_shared<MemTable>(++next_sst_id_);
        {
            std::unique_lock lock(state_mutex_);
            auto old_memtable = std::move(state_->memtable);
            state_->imm_memtables.insert(state_->imm_memtables.begin(), std::move(old_memtable));
            state_->memtable = new_memtable;
        }
    }

private:
    std::shared_ptr<LsmStorageState> state_;
    LsmStorageOptions options_;
    std::atomic<size_t> next_sst_id_;
    mutable std::shared_mutex state_mutex_;
};

// MiniLsm 结构体
class MiniLsm {
public:
    static std::shared_ptr<MiniLsm> open(const LsmStorageOptions& options) {
        return std::make_shared<MiniLsm>(options);
    }

    std::optional<Bytes> get(const Bytes& key) {
        return inner_->get(key);
    }

    void put(const Bytes& key, const Bytes& value) {
        inner_->put(key, value);
    }

    void force_flush() {
        inner_->force_freeze_memtable();
    }

private:
    explicit MiniLsm(const LsmStorageOptions& options) : inner_(std::make_shared<LsmStorageInner>(options)) {}
    std::shared_ptr<LsmStorageInner> inner_;
};
