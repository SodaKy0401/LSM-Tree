cmake_minimum_required(VERSION 3.10)
project(MyProject)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_STANDARD_REQUIRED YES)

# 添加 GTest（假设已经通过 package manager 安装）
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})
add_compile_options(-std=c++20)








add_library(skiplist STATIC 
    src/SkipList/skiplist.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/iterator.cpp 
)
target_include_directories(skiplist PUBLIC src/SkipList)


# 添加 Block 目录，并创建 block 库
add_library(block STATIC 
    src/Block/block.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/block_iterator.cpp 
)
target_include_directories(block PUBLIC src/Block)

add_library(blockcache STATIC 
    src/Block/block_cache.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/block_iterator.cpp 
)
target_include_directories(blockcache PUBLIC src/Block src/Iterator)

add_library(blockmeta STATIC 
    src/Block/blockmeta.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/block_iterator.cpp 
)
target_include_directories(blockmeta PUBLIC src/Block src/Iterator)

add_library(sst STATIC 
src/SST/sst.cpp
src/SST/sst_iterator.cpp
src/Utils/file.cpp
src/Block/block.cpp
src/Block/blockmeta.cpp  # 添加 BlockMeta 相关实现
src/Iterator/block_iterator.cpp # 可能的 BlockIterator 定义
src/Utils/bloom_filter.cpp
src/Block/block_cache.cpp
src/Utils/std_file.cpp
src/SkipList/skiplist.cpp  # 确保这里包含你的 .cpp 文件
src/Iterator/iterator.cpp 
   
)
target_include_directories(sst PUBLIC src/SST src/Utils src/Block src/Iterator) 



add_library(memtable STATIC 
    src/Memtable/memtable.cpp  # 确保这里包含你的 .cpp 文件
    src/Iterator/iterator.cpp 
    src/Iterator/block_iterator.cpp 
    src/Block/blockmeta.cpp
)
target_include_directories(memtable PUBLIC src/Memtable  src/Utils src/Block src/Iterator src/SkipList)

add_library(file STATIC 
    src/Utils/file.cpp  # 确保这里包含你的 .cpp 文件
    src/Utils/std_file.cpp  # 确保这里包含你的 .cpp 文件
    src/Utils/bloom_filter.cpp
)
target_include_directories(file PUBLIC src/Utils)

add_library(lsm STATIC 
    src/WAL/wal.cpp
    src/WAL/record.cpp
    src/LSM/engine.cpp  # 确保这里包含你的 .cpp 文件
    src/Compact/two_merge_iterator.cpp
    src/SST/concat_iterator.cpp
    src/LSM/level_iterator.cpp
    src/LSM/transaction.cpp
    
)
target_include_directories(lsm PUBLIC src/LSM src/Compact src/Utils src/Block src/Iterator src/WAL)




add_library(wal STATIC 
    # src/LSM/engine.cpp
    src/WAL/wal.cpp
    src/WAL/record.cpp
    src/SST/concat_iterator.cpp
    src/Utils/file.cpp
    src/Utils/std_file.cpp
    src/SST/concat_iterator.cpp
    src/LSM/level_iterator.cpp
    src/LSM/transaction.cpp
)
target_include_directories(wal PUBLIC src/LSM src/Compact src/Utils src/Block src/Iterator src/WAL)



# # 添加测试可执行文件
add_executable(test_block test/testblock.cpp)
add_executable(test_skiplist test/testskiplist.cpp)
add_executable(test_blockcache test/testblock_cache.cpp)
add_executable(test_memtable test/testmemtable.cpp)
add_executable(test_sst test/testsst.cpp)
add_executable(test_util test/testfile.cpp)
add_executable(testmeta test/testmeta.cpp)
add_executable(testlsm test/testlsm.cpp)
add_executable(testcomp test/testcompact.cpp)

 add_executable(testwal test/testwal.cpp)
# # 让 test_block 依赖 block 和 GTest




target_link_libraries(test_block PRIVATE block blockcache GTest::GTest GTest::Main)
target_link_libraries(test_blockcache PRIVATE block blockcache GTest::GTest GTest::Main)
target_link_libraries(test_skiplist PRIVATE skiplist GTest::GTest GTest::Main)
target_link_libraries(test_sst PRIVATE sst  GTest::GTest GTest::Main skiplist block) 
target_link_libraries(test_memtable PRIVATE skiplist block  memtable sst GTest::GTest GTest::Main)
target_link_libraries(test_util PRIVATE file GTest::GTest GTest::Main)
target_link_libraries(testmeta PRIVATE file blockmeta block blockcache  lsm sst GTest::GTest GTest::Main)
target_link_libraries(testlsm PRIVATE lsm sst file blockmeta block blockcache memtable skiplist GTest::GTest GTest::Main)
target_link_libraries(testcomp PRIVATE lsm sst file blockmeta block blockcache memtable skiplist GTest::GTest GTest::Main)
target_link_libraries(testwal PRIVATE lsm sst file blockmeta block blockcache memtable skiplist  wal GTest::GTest GTest::Main GTest::gmock)






enable_testing()





