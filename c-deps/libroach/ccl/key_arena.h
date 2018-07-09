// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <google/protobuf/arena.h>
#include <rocksdb/status.h>

// The returned Arena performs the following:
// Allocation:
// - mlock pages to prevent paging out
// - madvise(MADV_DONTDUMP) pages to prevent inclusion in core dumps
// Deallocation:
// - munlock pages
// - madvise(MADV_DODUMP) to re-enable inclusion in core dumps
// - overwrite memory with zeros
//
// mlock/munlock/madvise are only called once per page (using reference counting)
// because these calls don't stack on some systems.
// Memory overwrite with zeros is performed on every deallocation.
//
// Objects owned by the arena should not have their destructors run. Instead,
// deleting the arena deletes all the contained objects.
// Multiple arenas can be created. They will share the underlying page registry.
google::protobuf::Arena* NewKeyArena();

// CanLockPages returns OK iff:
// - we can determine the page size
// - mlock succeeds
// - madvise(MADV_DONTDUMP) succeeds
// This can be called at any time but should be done before any use of the arena.
rocksdb::Status CanLockPages();

// Returns true if all pages in [addr, addr+size) are locked.
bool IsLocked(size_t addr, size_t size);
