// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "key_arena.h"
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

// Defined here to allow testing. Only used internally.
class PageRegistry {
 public:
  PageRegistry() {}
  ~PageRegistry() {}

  // Lock the range of [addr, addr+size).
  void Lock(size_t addr, size_t size) { ModifyAddress(addr, size, true /* increment */); }

  // Unlock the range of [addr, addr+size).
  void Unlock(size_t addr, size_t size) {
    // Overwrite memory with random bytes.
    FillRandomBytes((unsigned char*)addr, size);
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
      it.first->second++;
      if (it.first->second > 1) {
        // We locked this page before.
        return;
      }
    } else {
      it.first->second--;
      if (it.first->second != 0) {
        // This page is still referenced.
        return;
      }
      locked_pages_.erase(it.first);
    }

    if (increment) {
      // We don't have a good way to surface mlock/madvise errors. Log to stdout instead.
      if (mlock((void*)page_start, kPageSize) != 0) {
        std::cout << "mlock failed with: " << strerror(errno) << std::endl;
      }

// Check for both defines otherwise we could not revert.
#if defined(MADV_DONTDUMP) && defined(MADV_DODUMP)
      if (madvise((void*)page_start, kPageSize, MADV_DONTDUMP) != 0) {
        std::cout << "madvise failed with: " << strerror(errno) << std::endl;
      }
#endif
    } else {
      if (munlock((void*)page_start, kPageSize) != 0) {
        std::cout << "mlock failed with: " << strerror(errno) << std::endl;
      }

// Execute only if we called DONTDUMP and if DODUMP is defined.
#if defined(MADV_DONTDUMP) && defined(MADV_DODUMP)
      if (madvise((void*)page_start, kPageSize, MADV_DODUMP) != 0) {
        std::cout << "madvise failed with: " << strerror(errno) << std::endl;
      }
#endif
    }
  }

  mutable std::mutex mu_;
  // Hash map of <page start address> -> <number of locks>.
  typedef std::unordered_map<size_t, int> LockedPageMap;
  LockedPageMap locked_pages_;
};

// Global page registry.
static PageRegistry pageRegistry;

// Try to lock/madvise and release a single page containing 'addr'.
rocksdb::Status TryLockUnlock(size_t addr) {
  addr -= (addr % kPageSize);

  if (mlock((void*)addr, kPageSize) != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to mlock page : %s", strerror(errno)));
  }

#ifdef MADV_DONTDUMP
  if (madvise((void*)addr, kPageSize, MADV_DONTDUMP) != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to madvise(MADV_DONTDUMP) page : %s", strerror(errno)));
  }
#else
  return rocksdb::Status::NotSupported("madvise(MADV_DONTDUMP) is not supported");
#endif

  if (munlock((void*)addr, kPageSize) != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to munlock page : %s", strerror(errno)));
  }

// We've already failed if !defined(MADV_DONTDUMP), don't check for both.
#ifdef MADV_DODUMP
  if (madvise((void*)addr, kPageSize, MADV_DODUMP) != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to madvise(MADV_DODUMP) page : %s", strerror(errno)));
  }
#else
  return rocksdb::Status::NotSupported("madvise(MADV_DODUMP) is not supported");
#endif

  return rocksdb::Status::OK();
}

rocksdb::Status CanLockPages() {
  long page_size = sysconf(_SC_PAGE_SIZE);
  if (page_size == -1) {
    return rocksdb::Status::NotSupported(fmt::StringPrintf(
        "failed to determine page size using sysconf(_SC_PAGE_SIZE): %s", strerror(errno)));
  }

  // Make a small allocation and try locking/unlocking.
  char* foo = new char[128];
  auto status = TryLockUnlock((size_t)foo);
  delete[] foo;

  return status;
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
