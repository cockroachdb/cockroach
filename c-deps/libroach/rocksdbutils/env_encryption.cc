// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
//
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found at
//  https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
//  and Apache 2.0 License (found in licenses/APL.txt in the root
//  of this repository).

#include <algorithm>

#include "../fmt.h"
#include "../plaintext_stream.h"
#include "aligned_buffer.h"
#include "env_encryption.h"

using namespace cockroach;

namespace rocksdb_utils {

class EncryptedSequentialFile : public rocksdb::SequentialFile {
 private:
  std::unique_ptr<rocksdb::SequentialFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  uint64_t offset_;

 public:
  // Default constructor.
  EncryptedSequentialFile(rocksdb::SequentialFile* f, BlockAccessCipherStream* s)
      : file_(f), stream_(s), offset_(0) {}

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual rocksdb::Status Read(size_t n, rocksdb::Slice* result, char* scratch) override {
    assert(scratch);
    rocksdb::Status status = file_->Read(n, result, scratch);
    if (!status.ok()) {
      return status;
    }
    status = stream_->Decrypt(offset_, (char*)result->data(), result->size());
    offset_ += result->size();  // We've already ready data from disk, so update offset_ even if
                                // decryption fails.
    return status;
  }

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual rocksdb::Status Skip(uint64_t n) override {
    auto status = file_->Skip(n);
    if (!status.ok()) {
      return status;
    }
    offset_ += n;
    return status;
  }

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override { return file_->use_direct_io(); }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override {
    return file_->GetRequiredBufferAlignment();
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    return file_->InvalidateCache(offset, length);
  }

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual rocksdb::Status PositionedRead(uint64_t offset, size_t n, rocksdb::Slice* result,
                                         char* scratch) override {
    assert(scratch);
    auto status = file_->PositionedRead(offset, n, result, scratch);
    if (!status.ok()) {
      return status;
    }
    offset_ = offset + result->size();
    status = stream_->Decrypt(offset, (char*)result->data(), result->size());
    return status;
  }
};

// A file abstraction for randomly reading the contents of a file.
class EncryptedRandomAccessFile : public rocksdb::RandomAccessFile {
 private:
  std::unique_ptr<rocksdb::RandomAccessFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;

 public:
  EncryptedRandomAccessFile(rocksdb::RandomAccessFile* f, BlockAccessCipherStream* s)
      : file_(f), stream_(s) {}

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  // If Direct I/O enabled, offset, n, and scratch should be aligned properly.
  virtual rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result,
                               char* scratch) const override {
    assert(scratch);
    auto status = file_->Read(offset, n, result, scratch);
    if (!status.ok()) {
      return status;
    }
    status = stream_->Decrypt(offset, (char*)result->data(), result->size());
    return status;
  }

  // Readahead the file starting from offset by n bytes for caching.
  virtual rocksdb::Status Prefetch(uint64_t offset, size_t n) override {
    // return rocksdb::Status::OK();
    return file_->Prefetch(offset, n);
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to eachother by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  virtual size_t GetUniqueId(char* id, size_t max_size) const override {
    return file_->GetUniqueId(id, max_size);
  };

  virtual void Hint(AccessPattern pattern) override { file_->Hint(pattern); }

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override { return file_->use_direct_io(); }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override {
    return file_->GetRequiredBufferAlignment();
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    return file_->InvalidateCache(offset, length);
  }
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class EncryptedWritableFile : public rocksdb::WritableFileWrapper {
 private:
  std::unique_ptr<rocksdb::WritableFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;

 public:
  // Default constructor.
  EncryptedWritableFile(rocksdb::WritableFile* f, BlockAccessCipherStream* s)
      : rocksdb::WritableFileWrapper(f), file_(f), stream_(s) {}

  rocksdb::Status Append(const rocksdb::Slice& data) override {
    AlignedBuffer buf;
    rocksdb::Status status;
    rocksdb::Slice dataToAppend(data);
    if (data.size() > 0) {
      auto offset = file_->GetFileSize();  // size including prefix
      // Encrypt in cloned buffer
      buf.Alignment(GetRequiredBufferAlignment());
      buf.AllocateNewBuffer(data.size());
      memmove(buf.BufferStart(), data.data(), data.size());
      status = stream_->Encrypt(offset, buf.BufferStart(), data.size());
      if (!status.ok()) {
        return status;
      }
      dataToAppend = rocksdb::Slice(buf.BufferStart(), data.size());
    }
    status = file_->Append(dataToAppend);
    if (!status.ok()) {
      return status;
    }
    return status;
  }

  rocksdb::Status PositionedAppend(const rocksdb::Slice& data, uint64_t offset) override {
    AlignedBuffer buf;
    rocksdb::Status status;
    rocksdb::Slice dataToAppend(data);
    if (data.size() > 0) {
      // Encrypt in cloned buffer
      buf.Alignment(GetRequiredBufferAlignment());
      buf.AllocateNewBuffer(data.size());
      memmove(buf.BufferStart(), data.data(), data.size());
      status = stream_->Encrypt(offset, buf.BufferStart(), data.size());
      if (!status.ok()) {
        return status;
      }
      dataToAppend = rocksdb::Slice(buf.BufferStart(), data.size());
    }
    status = file_->PositionedAppend(dataToAppend, offset);
    if (!status.ok()) {
      return status;
    }
    return status;
  }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const override { return file_->use_direct_io(); }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override {
    return file_->GetRequiredBufferAlignment();
  }
};

// A file abstraction for random reading and writing.
class EncryptedRandomRWFile : public rocksdb::RandomRWFile {
 private:
  std::unique_ptr<rocksdb::RandomRWFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;

 public:
  EncryptedRandomRWFile(rocksdb::RandomRWFile* f, BlockAccessCipherStream* s)
      : file_(f), stream_(s) {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const override { return file_->use_direct_io(); }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const override {
    return file_->GetRequiredBufferAlignment();
  }

  // Write bytes in `data` at  offset `offset`, Returns rocksdb::Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual rocksdb::Status Write(uint64_t offset, const rocksdb::Slice& data) override {
    AlignedBuffer buf;
    rocksdb::Status status;
    rocksdb::Slice dataToWrite(data);
    if (data.size() > 0) {
      // Encrypt in cloned buffer
      buf.Alignment(GetRequiredBufferAlignment());
      buf.AllocateNewBuffer(data.size());
      memmove(buf.BufferStart(), data.data(), data.size());
      status = stream_->Encrypt(offset, buf.BufferStart(), data.size());
      if (!status.ok()) {
        return status;
      }
      dataToWrite = rocksdb::Slice(buf.BufferStart(), data.size());
    }
    status = file_->Write(offset, dataToWrite);
    return status;
  }

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns rocksdb::Status::OK() on success.
  virtual rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result,
                               char* scratch) const override {
    assert(scratch);
    auto status = file_->Read(offset, n, result, scratch);
    if (!status.ok()) {
      return status;
    }
    status = stream_->Decrypt(offset, (char*)result->data(), result->size());
    return status;
  }

  virtual rocksdb::Status Flush() override { return file_->Flush(); }

  virtual rocksdb::Status Sync() override { return file_->Sync(); }

  virtual rocksdb::Status Fsync() override { return file_->Fsync(); }

  virtual rocksdb::Status Close() override { return file_->Close(); }
};

// EncryptedEnv implements an Env wrapper that adds encryption to files stored on disk.
class EncryptedEnv : public rocksdb::EnvWrapper {
 public:
  EncryptedEnv(rocksdb::Env* base_env, FileRegistry* file_registry, CipherStreamCreator* creator)
      : rocksdb::EnvWrapper(base_env), file_registry_(file_registry), stream_creator_(creator) {}

  // NewSequentialFile opens a file for sequential reading.
  virtual rocksdb::Status NewSequentialFile(const std::string& fname,
                                            std::unique_ptr<rocksdb::SequentialFile>* result,
                                            const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_reads) {
      return rocksdb::Status::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::SequentialFile> underlying;
    auto status = rocksdb::EnvWrapper::NewSequentialFile(fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = CreateCipherStream(fname, false /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::SequentialFile>(
        new EncryptedSequentialFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  // NewRandomAccessFile opens a file for random read access.
  virtual rocksdb::Status NewRandomAccessFile(const std::string& fname,
                                              std::unique_ptr<rocksdb::RandomAccessFile>* result,
                                              const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_reads) {
      return rocksdb::Status::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::RandomAccessFile> underlying;
    auto status = rocksdb::EnvWrapper::NewRandomAccessFile(fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = CreateCipherStream(fname, false /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::RandomAccessFile>(
        new EncryptedRandomAccessFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  // NewWritableFile opens a file for sequential writing.
  virtual rocksdb::Status NewWritableFile(const std::string& fname,
                                          std::unique_ptr<rocksdb::WritableFile>* result,
                                          const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_writes) {
      return rocksdb::Status::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::WritableFile> underlying;
    rocksdb::Status status = rocksdb::EnvWrapper::NewWritableFile(fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = CreateCipherStream(fname, true /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::WritableFile>(
        new EncryptedWritableFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual rocksdb::Status ReopenWritableFile(const std::string& fname,
                                             std::unique_ptr<rocksdb::WritableFile>* result,
                                             const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_writes) {
      return rocksdb::Status::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::WritableFile> underlying;
    rocksdb::Status status = rocksdb::EnvWrapper::ReopenWritableFile(fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = CreateCipherStream(fname, true /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::WritableFile>(
        new EncryptedWritableFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  // `ReuseWritableFile` is used for recycling WAL files. With `EncryptedEnv`, we
  // currently do not know a crash-safe way to recycle WALs. The workaround is to
  // pretend to recycle in this function by producing a new file and throwing away
  // the old one. This method of "recycling" sacrifices performance for crash-safety.
  //
  // True recycling is not crash-safe because RocksDB recovery fails if it encounters
  // a non-empty WAL with zero readable entries. With true recycling, we need to change
  // the encryption key. Imagine a crash happened after we renamed the file and assigned
  // a new key, but before we wrote any data to the recycled file. Then, that file would
  // appear to recovery as containing all garbage data, causing it to fail.
  //
  // TODO(ajkr): Try to change how RocksDB handles WALs with zero readable entries:
  // https://www.facebook.com/groups/rocksdb.dev/permalink/2405909486174218/
  // Then we can have both performance and crash-safety.
  virtual rocksdb::Status ReuseWritableFile(const std::string& fname, const std::string& old_fname,
                                            std::unique_ptr<rocksdb::WritableFile>* result,
                                            const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_writes) {
      return rocksdb::Status::InvalidArgument();
    }
    auto status = NewWritableFile(fname, result, options);
    if (status.ok()) {
      status = DeleteFile(old_fname);
    }
    return status;
  }

  // Open `fname` for random read and write, if file dont exist the file
  // will be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual rocksdb::Status NewRandomRWFile(const std::string& fname,
                                          std::unique_ptr<rocksdb::RandomRWFile>* result,
                                          const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_reads || options.use_mmap_writes) {
      return rocksdb::Status::InvalidArgument();
    }
    // Check file exists
    bool isNewFile = !FileExists(fname).ok();

    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::RandomRWFile> underlying;
    rocksdb::Status status = rocksdb::EnvWrapper::NewRandomRWFile(fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream, indicating whether this is a new file.
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = CreateCipherStream(fname, isNewFile /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::RandomRWFile>(
        new EncryptedRandomRWFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  virtual rocksdb::Status DeleteFile(const std::string& fname) override {
    auto status = EnvWrapper::DeleteFile(fname);
    if (!status.ok()) {
      return status;
    }
    return file_registry_->MaybeDeleteEntry(fname);
  }

  virtual rocksdb::Status RenameFile(const std::string& src, const std::string& target) override {
    auto status = EnvWrapper::RenameFile(src, target);
    if (!status.ok()) {
      return status;
    }
    return file_registry_->MaybeRenameEntry(src, target);
  }

  virtual rocksdb::Status LinkFile(const std::string& src, const std::string& target) override {
    auto status = EnvWrapper::LinkFile(src, target);
    if (!status.ok()) {
      return status;
    }
    return file_registry_->MaybeLinkEntry(src, target);
  }

  rocksdb::Status
  CreateCipherStream(const std::string& fname, bool new_file,
                     std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
    if (new_file) {
      return InitAndCreateCipherStream(fname, result);
    } else {
      return LookupAndCreateCipherStream(fname, result);
    }
  }

  rocksdb::Status LookupAndCreateCipherStream(const std::string& fname,
                                              std::unique_ptr<BlockAccessCipherStream>* result) {

    // Look for file in registry.
    auto file_entry = file_registry_->GetFileEntry(fname);
    if (file_entry == nullptr) {
      // No entry: plaintext.
      (*result) = std::unique_ptr<BlockAccessCipherStream>(new PlaintextStream());
      return rocksdb::Status::OK();
    }

    // File entry exists: check that the env_type corresponds to the requested one.
    if (file_entry->env_type() != stream_creator_->GetEnvType()) {
      return rocksdb::Status::InvalidArgument(
          fmt::StringPrintf("file %s was written using env_type %d, but we are trying to read it "
                            "using env_type %d: problems will occur",
                            fname.c_str(), file_entry->env_type(), stream_creator_->GetEnvType()));
    }

    return stream_creator_->CreateCipherStreamFromSettings(file_entry->encryption_settings(),
                                                           result);
  }

  rocksdb::Status InitAndCreateCipherStream(const std::string& fname,
                                            std::unique_ptr<BlockAccessCipherStream>* result) {

    // Look for file in registry.
    auto file_entry = file_registry_->GetFileEntry(fname);
    if (file_entry != nullptr && file_entry->env_type() != stream_creator_->GetEnvType()) {
      // File entry exists but the env_type does not match the requested one.
      return rocksdb::Status::InvalidArgument(
          fmt::StringPrintf("file %s was written using env_type %d, but we are overwriting it "
                            "using env_type %d: problems will occur",
                            fname.c_str(), file_entry->env_type(), stream_creator_->GetEnvType()));
    }

    std::string encryption_settings;
    auto status = stream_creator_->InitSettingsAndCreateCipherStream(&encryption_settings, result);
    if (!status.ok()) {
      return status;
    }

    // Create a FileEntry and save it, overwriting any existing one.
    std::unique_ptr<enginepb::FileEntry> new_entry(new enginepb::FileEntry());
    new_entry->set_env_type(stream_creator_->GetEnvType());
    new_entry->set_encryption_settings(encryption_settings);
    return file_registry_->SetFileEntry(fname, std::move(new_entry));
  }

 private:
  FileRegistry* file_registry_;
  std::unique_ptr<CipherStreamCreator> stream_creator_;
};

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
rocksdb::Env* NewEncryptedEnv(rocksdb::Env* base_env, FileRegistry* file_registry,
                              CipherStreamCreator* creator) {
  return new EncryptedEnv(base_env, file_registry, creator);
}

// Encrypt one or more (partial) blocks of data at the file offset.
// Length of data is given in dataSize.
rocksdb::Status BlockAccessCipherStream::Encrypt(uint64_t fileOffset, char* data,
                                                 size_t dataSize) const {
  if (dataSize == 0) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<rocksdb_utils::BlockCipher> cipher;
  auto status = InitCipher(&cipher);
  if (!status.ok()) {
    return status;
  }

  // Calculate block index
  auto blockSize = cipher->BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  scratch.reserve(blockSize);

  // Encrypt individual blocks.
  while (1) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not encrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer.get()) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy plain data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = EncryptBlock(cipher.get(), blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy encrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }
    dataSize -= n;
    if (dataSize == 0) {
      return rocksdb::Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

// Decrypt one or more (partial) blocks of data at the file offset.
// Length of data is given in dataSize.
rocksdb::Status BlockAccessCipherStream::Decrypt(uint64_t fileOffset, char* data,
                                                 size_t dataSize) const {
  if (dataSize == 0) {
    return rocksdb::Status::OK();
  }

  std::unique_ptr<rocksdb_utils::BlockCipher> cipher;
  auto status = InitCipher(&cipher);
  if (!status.ok()) {
    return status;
  }

  // Calculate block index
  auto blockSize = cipher->BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  scratch.reserve(blockSize);

  // Decrypt individual blocks.
  while (1) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not decrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer.get()) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy encrypted data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = DecryptBlock(cipher.get(), blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy decrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }
    dataSize -= n;
    if (dataSize == 0) {
      return rocksdb::Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

}  // namespace rocksdb_utils
