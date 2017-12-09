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
# Assuming no extra dependencies/includes/source files, you can just add your test source
# to one of the two lists below:

# Tests that link libroach.
set(tests
  libroach_test.cc
)

# Tests that link libroachccl.
set (ccl_tests
  ccl/libroach_ccl_test.cc
)

set(all_tests ${tests} ${ccl_tests})

# "test" doesn't depend on the actual tests. Let's add a "check" target
# that depends on all test executables and runs "ctest".
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

foreach(tsrc ${all_tests})
  get_filename_component(tname ${tsrc} NAME_WE)
  add_executable(${tname} ${tsrc})
  add_test(${tname} ${tname})
	add_dependencies(check ${tname})
endforeach(tsrc)

foreach(tsrc ${tests})
  get_filename_component(tname ${tsrc} NAME_WE)
  target_link_libraries(${tname} roach)
endforeach(tsrc)

foreach(tsrc ${ccl_tests})
  get_filename_component(tname ${tsrc} NAME_WE)
  target_link_libraries(${tname} roachccl)
endforeach(tsrc)
