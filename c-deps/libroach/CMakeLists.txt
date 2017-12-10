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

# The CXX_STANDARD property was introduced in version 3.1.
cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

project(roachlib)

add_library(roach
  db.cc
  encoding.cc
  env_switching.cc
  eventlistener.cc
  protos/roachpb/data.pb.cc
  protos/roachpb/internal.pb.cc
  protos/roachpb/metadata.pb.cc
  protos/storage/engine/enginepb/mvcc.pb.cc
  protos/storage/engine/enginepb/mvcc3.pb.cc
  protos/storage/engine/enginepb/registry.pb.cc
  protos/storage/engine/enginepb/rocksdb.pb.cc
  protos/util/hlc/legacy_timestamp.pb.cc
  protos/util/hlc/timestamp.pb.cc
  protos/util/unresolved_addr.pb.cc
)
target_include_directories(roach
  PUBLIC  ./include
  PRIVATE ../protobuf/src ../rocksdb/include protos
)

add_library(roachccl
  ccl/db.cc
)
target_include_directories(roachccl
  PRIVATE ../rocksdb/include
)
target_link_libraries(roachccl roach)

set_target_properties(roach roachccl PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
  COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare"
)

enable_testing()

# Tests executables are named after the source file (eg: libroach_test for libroach_test.cc)
# Test names are the same as test executables.
set(tests
  encoding_test.cc
  libroach_test.cc
  ccl/libroach_ccl_test.cc
)

# "test" doesn't depend on the actual tests. Let's add a "check" target
# that depends on all test executables and runs "ctest".
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -V)

add_subdirectory(../googletest/googletest
                 ${CMAKE_BINARY_DIR}/googletest
                 EXCLUDE_FROM_ALL)

# This is a known problem with googletest. Just doing what the internet says.
set(THREADS_PTHREAD_ARG "2" CACHE STRING "Forcibly set by CMakeLists.txt." FORCE)

foreach(tsrc ${tests})
  get_filename_component(filename ${tsrc} NAME_WE)
  get_filename_component(dirname ${tsrc} DIRECTORY)
  if("${dirname}" STREQUAL "" )
    set(tname ${filename})
  else()
    set(tname ${dirname}_${filename})
  endif()

  # Add a new executable and set includes/libraries/properties.
  add_executable(${tname} ${tsrc})
  target_include_directories(${tname}
    PRIVATE ../googletest/googletest/include
    PRIVATE ../rocksdb/include
  )
  target_link_libraries(${tname} roachccl gtest_main pthread)

  set_target_properties(${tname} PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
    COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare"
  )

  # Add the executable to the set of tests run by the "check" target.
  add_test(${tname} ${tname})
  add_dependencies(check ${tname})
endforeach(tsrc)
