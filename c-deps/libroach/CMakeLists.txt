# Copyright 2017 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# NB: Despite CMake's portability, this build configuration makes no attempt to
# support non-GCC-like compilers.

# The CXX_STANDARD property was introduced in version 3.1
# 3.3 fixes https://cmake.org/cmake/help/v3.3/policy/CMP0060.html
cmake_minimum_required(VERSION 3.3 FATAL_ERROR)

project(roachlib)

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
endif()

add_library(roach
  batch.cc
  cache.cc
  chunked_buffer.cc
  comparator.cc
  db.cc
  encoding.cc
  engine.cc
  eventlistener.cc
  file_registry.cc
  getter.cc
  godefs.cc
  incremental_iterator.cc
  row_counter.cc
  iterator.cc
  ldb.cc
  merge.cc
  mvcc.cc
  options.cc
  snapshot.cc
  sst_dump.cc
  stack_trace.cc
  table_props.cc
  utils.cc
  protos/kv/kvserver/concurrency/lock/locking.pb.cc
  protos/roachpb/api.pb.cc
  protos/roachpb/data.pb.cc
  protos/roachpb/errors.pb.cc
  protos/roachpb/internal.pb.cc
  protos/roachpb/metadata.pb.cc
  protos/storage/enginepb/mvcc.pb.cc
  protos/storage/enginepb/mvcc3.pb.cc
  protos/storage/enginepb/file_registry.pb.cc
  protos/storage/enginepb/rocksdb.pb.cc
  protos/util/hlc/legacy_timestamp.pb.cc
  protos/util/hlc/timestamp.pb.cc
  protos/util/log/log.pb.cc
  protos/util/tracing/recorded_span.pb.cc
  protos/util/unresolved_addr.pb.cc
  rocksdbutils/env_encryption.cc
)
target_include_directories(roach
  PUBLIC  ./include
  PRIVATE ../protobuf/src
  PRIVATE ../rocksdb/include
  PRIVATE protos
)

add_library(roachccl
  ccl/crypto_utils.cc
  ccl/ctr_stream.cc
  ccl/db.cc
  ccl/key_manager.cc
  protosccl/ccl/baseccl/encryption_options.pb.cc
  protosccl/ccl/storageccl/engineccl/enginepbccl/key_registry.pb.cc
  protosccl/ccl/storageccl/engineccl/enginepbccl/stats.pb.cc
)
target_include_directories(roachccl
  PUBLIC ./ccl/include
  PRIVATE .. # CryptoPP headers are directly in the directory. Include .. to be able to include <cryptopp/....h>
  PRIVATE ../protobuf/src
  PRIVATE ../rocksdb/include
  PRIVATE protos
  PRIVATE protosccl
)
target_link_libraries(roachccl roach)

set_target_properties(roach roachccl PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
  COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare;-Wno-array-bounds"
)

enable_testing()

# List of tests to build and run. Tests in `ccl/` are linked against roachccl, all others
# are linked against roach only.
set(tests
  chunked_buffer_test.cc
  db_test.cc
  comparator_test.cc
  encoding_test.cc
  file_registry_test.cc
  merge_test.cc
  ccl/crypto_utils_test.cc
  ccl/db_test.cc
  ccl/encrypted_env_test.cc
  ccl/key_manager_test.cc
)

# "test" doesn't depend on the actual tests. Let's add a "check" target
# that depends on all test executables and runs "ctest".
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -V)

# Add googletest and get around a weird cmake issue:
# https://gitlab.kitware.com/cmake/cmake/issues/16920
set(THREADS_PTHREAD_ARG "2" CACHE STRING "Forcibly set by CMakeLists.txt." FORCE)
add_subdirectory(../googletest/googletest
                 ${CMAKE_BINARY_DIR}/googletest
                 EXCLUDE_FROM_ALL)

# TODO(benesch): make this required when CMake 3.9 is widely deployed.
include(GoogleTest OPTIONAL)

# Iterate over all test sources.
foreach(tsrc ${tests})
  # Build target name from filename (eg: ccl_db_test.cc for ccl/db_test.cc).
  get_filename_component(filename ${tsrc} NAME_WE)
  get_filename_component(dirname ${tsrc} DIRECTORY)
  if("${dirname}" STREQUAL "" )
    set(tname ${filename})
  else()
    set(tname ${dirname}_${filename})
  endif()

  if(${tsrc} MATCHES "^ccl/")
    # Link `ccl/` tests against roachccl and CryptoPP.
		# Use ccl/testutils.
    add_executable(${tname} ${tsrc} testutils.cc ccl/testutils.cc)
    target_link_libraries(${tname}
      roachccl
      ${CRYPTOPP_LIB}
    )
    target_include_directories(${tname}
      PRIVATE .. # CryptoPP headers are directly in the directory. Include .. to be able to include <cryptopp/....h>
      PRIVATE protosccl
    )
  else()
		# Use testutils.
    add_executable(${tname} ${tsrc} testutils.cc)
  endif()

	# Set includes/libraries/properties.
  target_include_directories(${tname}
    PRIVATE ../googletest/googletest/include
    PRIVATE ../protobuf/src
    PRIVATE ../rocksdb/include
    PRIVATE protos
  )

  # Add all other libraries.
  target_link_libraries(${tname}
    roach
    gtest_main
    pthread
    ${ROCKSDB_LIB}
    ${PROTOBUF_LIB}
    ${JEMALLOC_LIB}
    ${SNAPPY_LIB}
  )

  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    target_link_libraries(${tname} rt)
  endif()

  set_target_properties(${tname} PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
    COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare;-Wno-array-bounds"
  )

  # Add the executable to the set of tests run by the "check" target.
  if(COMMAND gtest_discover_tests)
    # gtest_discover_tests, introduced in CMake 3.10, teaches CTest about the
    # actual test cases within the test binary.
    gtest_discover_tests(${tname})
  else()
    # In earlier versions, just tell CTest to treat the test binary as a black
    # box that returns an exit code.
    add_test(${tname} ${tname})
  endif()
  add_dependencies(check ${tname})
endforeach(tsrc)
