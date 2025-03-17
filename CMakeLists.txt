cmake_minimum_required(VERSION 3.10)
project(MyProject)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_STANDARD_REQUIRED YES)

# 添加 GTest（假设已经通过 package manager 安装）
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})
add_compile_options(-std=c++20)





# 添加 Block 目录，并创建 block 库
add_library(block STATIC 
    src/Block/block.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/block_iterator.cpp 
)
target_include_directories(block PUBLIC src/Block)

add_library(skiplist STATIC 
    src/SkipList/skiplist.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/iterator.cpp 
)
target_include_directories(skiplist PUBLIC src/SkipList)

# 添加测试可执行文件
add_executable(test_block test/testblock.cpp)
add_executable(test_skiplist test/testskiplist.cpp)

# 让 test_block 依赖 block 和 GTest

target_link_libraries(test_block PRIVATE block GTest::GTest GTest::Main)

target_link_libraries(test_skiplist PRIVATE skiplist GTest::GTest GTest::Main)

enable_testing()
add_test(NAME test_block COMMAND test_block)
add_test(NAME test_skiplist COMMAND test_skiplist)