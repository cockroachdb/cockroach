// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found at
//  https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
//  and Apache 2.0 License (found in licenses/APL.txt in the root
//  of this repository).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the licences/LevelDB.txt file in in the root of this repository.
// See the LevelDB.AUTHORS file for names of contributors.
#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>

namespace rocksdb_utils {

// Needed parts from rocksdb/port/win/port_win.h
// VS < 2015
#if defined(OS_WIN) && defined(_MSC_VER) && (_MSC_VER < 1900)
#define ROCKSDB_NOEXCEPT
#else  // VS >= 2015 or MinGW
#define ROCKSDB_NOEXCEPT noexcept
#endif

inline size_t TruncateToPageBoundary(size_t page_size, size_t s) {
  s -= (s & (page_size - 1));
  assert((s % page_size) == 0);
  return s;
}

inline size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

// This class is to manage an aligned user
// allocated buffer for direct I/O purposes
// though can be used for any purpose.
class AlignedBuffer {
  size_t alignment_;
  std::unique_ptr<char[]> buf_;
  size_t capacity_;
  size_t cursize_;
  char* bufstart_;

 public:
  AlignedBuffer() : alignment_(), capacity_(0), cursize_(0), bufstart_(nullptr) {}

  AlignedBuffer(AlignedBuffer&& o) ROCKSDB_NOEXCEPT { *this = std::move(o); }

  AlignedBuffer& operator=(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    alignment_ = std::move(o.alignment_);
    buf_ = std::move(o.buf_);
    capacity_ = std::move(o.capacity_);
    cursize_ = std::move(o.cursize_);
    bufstart_ = std::move(o.bufstart_);
    return *this;
  }

  AlignedBuffer(const AlignedBuffer&) = delete;

  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  static bool isAligned(const void* ptr, size_t alignment) {
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
  }

  static bool isAligned(size_t n, size_t alignment) { return n % alignment == 0; }

  size_t Alignment() const { return alignment_; }

  size_t Capacity() const { return capacity_; }

  size_t CurrentSize() const { return cursize_; }

  const char* BufferStart() const { return bufstart_; }

  char* BufferStart() { return bufstart_; }

  void Clear() { cursize_ = 0; }

  void Alignment(size_t alignment) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    alignment_ = alignment;
  }

  // Allocates a new buffer and sets bufstart_ to the aligned first byte
  void AllocateNewBuffer(size_t requested_capacity, bool copy_data = false) {
    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    if (copy_data && requested_capacity < cursize_) {
      // If we are downsizing to a capacity that is smaller than the current
      // data in the buffer. Ignore the request.
      return;
    }

    size_t new_capacity = Roundup(requested_capacity, alignment_);
    char* new_buf = new char[new_capacity + alignment_];
    char* new_bufstart =
        reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(new_buf) + (alignment_ - 1)) &
                                ~static_cast<uintptr_t>(alignment_ - 1));

    if (copy_data) {
      memcpy(new_bufstart, bufstart_, cursize_);
    } else {
      cursize_ = 0;
    }

    bufstart_ = new_bufstart;
    capacity_ = new_capacity;
    buf_.reset(new_buf);
  }
  // Used for write
  // Returns the number of bytes appended
  size_t Append(const char* src, size_t append_size) {
    size_t buffer_remaining = capacity_ - cursize_;
    size_t to_copy = std::min(append_size, buffer_remaining);

    if (to_copy > 0) {
      memcpy(bufstart_ + cursize_, src, to_copy);
      cursize_ += to_copy;
    }
    return to_copy;
  }

  size_t Read(char* dest, size_t offset, size_t read_size) const {
    assert(offset < cursize_);

    size_t to_read = 0;
    if (offset < cursize_) {
      to_read = std::min(cursize_ - offset, read_size);
    }
    if (to_read > 0) {
      memcpy(dest, bufstart_ + offset, to_read);
    }
    return to_read;
  }

  /// Pad to alignment
  void PadToAlignmentWith(int padding) {
    size_t total_size = Roundup(cursize_, alignment_);
    size_t pad_size = total_size - cursize_;

    if (pad_size > 0) {
      assert((pad_size + cursize_) <= capacity_);
      memset(bufstart_ + cursize_, padding, pad_size);
      cursize_ += pad_size;
    }
  }

  // After a partial flush move the tail to the beginning of the buffer
  void RefitTail(size_t tail_offset, size_t tail_size) {
    if (tail_size > 0) {
      memmove(bufstart_, bufstart_ + tail_offset, tail_size);
    }
    cursize_ = tail_size;
  }

  // Returns place to start writing
  char* Destination() { return bufstart_ + cursize_; }

  void Size(size_t cursize) { cursize_ = cursize; }
};
}  // namespace rocksdb_utils
