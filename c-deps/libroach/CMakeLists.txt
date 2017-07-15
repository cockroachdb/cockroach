cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

project(roachlib)

add_library(roach
  db.cc
  encoding.cc
  eventlistener.cc
  protos/cockroach/pkg/roachpb/data.pb.cc
  protos/cockroach/pkg/roachpb/internal.pb.cc
  protos/cockroach/pkg/roachpb/metadata.pb.cc
  protos/cockroach/pkg/storage/engine/enginepb/mvcc.pb.cc
  protos/cockroach/pkg/storage/engine/enginepb/rocksdb.pb.cc
  protos/cockroach/pkg/util/hlc/timestamp.pb.cc
  protos/cockroach/pkg/util/unresolved_addr.pb.cc
)

add_library(roachccl
  ccl/db.cc
)

target_include_directories(roach
  PUBLIC  ./include
  PRIVATE ../protobuf/src ../rocksdb/include protos)

target_include_directories(roachccl
  PUBLIC ../rocksdb/include)
target_link_libraries(roachccl roach)

set_target_properties(roach roachccl PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)
