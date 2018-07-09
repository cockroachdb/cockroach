// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "key_arena.h"
#include <cstring>
#include <iostream>
#include <unordered_map>
#include "../fmt.h"
#include "crypto_utils.h"

#ifdef _WIN32
google::protobuf::Arena* NewKeyArena() {
  // We use an arena without custom alloc/dealloc functions.
  // This is because we still need someone to clean up memory.
  return new google::protobuf::Arena();
}

rocksdb::Status CanLockPages() {
  return rocksdb::Status::NotSupported(
      "preventing swapping and core dump of memory pages is not supported on Windows");
}

bool IsLocked(size_t addr, size_t size) { return false; }
#else  // ifdef _WIN32

#include <sys/mman.h>
#include <unistd.h>

// Determine page size at startup.
static const size_t kPageSize = (size_t)sysconf(_SC_PAGE_SIZE);

// mlock and maybe madvise the page starting at 'addr'.
rocksdb::Status LockPage(void* page_start) {
  rocksdb::Status status;

  if (mlock(page_start, kPageSize) != 0) {
    status = rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to mlock page: %s", strerror(errno)));
  }

// Check for both defines otherwise we could not revert.
#if defined(MADV_DONTDUMP) && defined(MADV_DODUMP)
  if (madvise(page_start, kPageSize, MADV_DONTDUMP) != 0) {
    status = rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to madvise(MADV_DONTDUMP) page: %s", strerror(errno)));
  }
#else
  status =
      rocksdb::Status::NotSupported("madvise does not support flags MADV_DONTDUMP or MADV_DODUMP");
#endif

  return status;
}

// munlock and maybe madvise the page starting at 'addr'.
rocksdb::Status UnlockPage(void* page_start) {
  rocksdb::Status status;

  if (munlock(page_start, kPageSize) != 0) {
    status = rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to munlock page: %s", strerror(errno)));
  }

// Execute only if we called DONTDUMP and if DODUMP is defined.
#if defined(MADV_DONTDUMP) && defined(MADV_DODUMP)
  if (madvise(page_start, kPageSize, MADV_DODUMP) != 0) {
    status = rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to madvise(MADV_DODUMP) page: %s", strerror(errno)));
  }
// Do not repeat the "madvise not supported" errors, it's caught in LockPage.
#endif

  return status;
}

class PageRegistry {
 public:
  PageRegistry() {}
  ~PageRegistry() {}

  // Lock the range of [addr, addr+size).
  void Lock(size_t addr, size_t size) { ModifyAddress(addr, size, true /* increment */); }

  // Unlock the range of [addr, addr+size).
  void Unlock(size_t addr, size_t size) {
    // Zero memory.
    std::memset((void*)addr, 0, size);
    // Decrease lock counter.
    ModifyAddress(addr, size, false /* increment */);
  }

  // Returns true if all pages in [addr, addr+size) are locked.
  bool IsLocked(size_t addr, size_t size) const {
    size_t end = addr + size;
    std::unique_lock<std::mutex> l(mu_);

    // Iterate through all pages.
    for (size_t page_start = addr - (addr % kPageSize); page_start < end; page_start += kPageSize) {
      auto it = locked_pages_.find(page_start);
      if (it == locked_pages_.cend() || it->second == 0) {
        return false;
      }
    }
    return true;
  }

 private:
  void ModifyAddress(size_t addr, size_t size, bool increment) {
    size_t end = addr + size;
    std::unique_lock<std::mutex> l(mu_);
    // Iterate through all pages.
    for (size_t page_start = addr - (addr % kPageSize); page_start < end; page_start += kPageSize) {
      ModifyPageLocked(page_start, increment);
    }
  }

  void ModifyPageLocked(size_t page_start, bool increment) {
    std::pair<LockedPageMap::iterator, bool> it =
        locked_pages_.insert(std::make_pair(page_start, 0));

    if (increment) {
      if (it.first->second == 0) {
        // First time this page is used: lock it.
        auto status = LockPage((void*)page_start);
        if (!status.ok()) {
          // We don't have a good way to surface errors here: log it.
          std::cerr << status.getState() << std::endl;
        }
      }
      it.first->second++;
    } else {
      it.first->second--;
      if (it.first->second == 0) {
        // Last reference to this page: unlock it and delete the entry.
        auto status = UnlockPage((void*)page_start);
        if (!status.ok()) {
          // We don't have a good way to surface errors here: log it.
          std::cerr << status.getState() << std::endl;
        }
        locked_pages_.erase(it.first);
      }
    }
  }

  mutable std::mutex mu_;
  // Hash map of <page start address> -> <number of locks>.
  typedef std::unordered_map<size_t, int> LockedPageMap;
  LockedPageMap locked_pages_;
};

// Global page registry.
static PageRegistry pageRegistry;

rocksdb::Status CanLockPages() {
  long page_size = sysconf(_SC_PAGE_SIZE);
  if (page_size == -1) {
    return rocksdb::Status::NotSupported(fmt::StringPrintf(
        "failed to determine page size using sysconf(_SC_PAGE_SIZE): %s", strerror(errno)));
  }

  // Make a small allocation and try locking/unlocking.
  char* foo = new char[128];
  auto page_addr = (size_t)foo;
  page_addr -= (page_addr % kPageSize);

  auto lock_status = LockPage((void*)page_addr);
  delete[] foo;
  // Always unlock regardless of lock_status. We may have failed in madvise but still need
  // to unlock the page.
  auto unlock_status = UnlockPage((void*)page_addr);

  if (!lock_status.ok()) {
    return lock_status;
  }
  return unlock_status;
}

bool IsLocked(size_t addr, size_t size) { return pageRegistry.IsLocked(addr, size); }

void* custom_alloc(size_t size) {
  void* object = ::operator new(size);
  pageRegistry.Lock((size_t)object, size);
  return object;
}

void custom_dealloc(void* object, size_t size) {
  // TODO(mberhault): use delete(object, size) once available (C++14 and higher).
  pageRegistry.Unlock((size_t)object, size);
  ::operator delete(object);
}

google::protobuf::Arena* NewKeyArena() {
  google::protobuf::ArenaOptions* opts = new google::protobuf::ArenaOptions();
  opts->block_alloc = &custom_alloc;
  opts->block_dealloc = &custom_dealloc;
  return new google::protobuf::Arena(*opts);
}

#endif  // ifndef _WIN32
