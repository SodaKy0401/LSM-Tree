#include "files.h"
#include <cstring>
#include <stdexcept>

FileObj::FileObj() : m_file(std::make_unique<StdFile>()) {}

FileObj::~FileObj() = default;

// 实现移动语义
FileObj::FileObj(FileObj &&other) noexcept
    : m_file(std::move(other.m_file)), m_size(other.m_size) {
  other.m_size = 0;
}

FileObj &FileObj::operator=(FileObj &&other) noexcept {
  if (this != &other) {
    m_file = std::move(other.m_file);
    m_size = other.m_size;
    other.m_size = 0;
  }
  return *this;
}

size_t FileObj::size() const { return m_file->size(); }

void FileObj::set_size(size_t size) { m_size = size; }

void FileObj::del_file() { m_file->remove(); }
FileObj FileObj::create_and_write(const std::string &path,
                                  std::vector<uint8_t> buf) {
  FileObj file_obj;
  if (!file_obj.m_file->create(path, buf)) {
    throw std::runtime_error("Failed to create or write file: " + path);
  }

  // 同步到磁盘
  file_obj.m_file->sync();

  return std::move(file_obj);
}

FileObj FileObj::open(const std::string &path) {
  FileObj file_obj;

  // 打开文件
  if (!file_obj.m_file->open(path, false)) {
    throw std::runtime_error("Failed to open file: " + path);
  }

  return std::move(file_obj);
}

std::vector<uint8_t> FileObj::read_to_slice(size_t offset, size_t length) {
  // 检查边界
  if (offset + length > m_file->size()) {
    throw std::out_of_range("Read beyond file size");
  }

  // 从w文件复制数据
  auto result = m_file->read(offset, length);

  return result;
}