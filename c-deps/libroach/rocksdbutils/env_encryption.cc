//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in this directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in this directory).

#include <algorithm>

#include "aligned_buffer.h"
#include "env_encryption.h"

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
  EncryptedEnv(rocksdb::Env* base_env, EncryptionProvider* provider, enginepb::EnvLevel env_level)
      : rocksdb::EnvWrapper(base_env), provider_(provider), env_level_(env_level) {}

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
    status = provider_->CreateCipherStream(env_level_, fname, false /* new_file */, &stream);
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
    status = provider_->CreateCipherStream(env_level_, fname, false /* new_file */, &stream);
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
    status = provider_->CreateCipherStream(env_level_, fname, true /* new_file */, &stream);
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
    status = provider_->CreateCipherStream(env_level_, fname, true /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::WritableFile>(
        new EncryptedWritableFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  // Reuse an existing file by renaming it and opening it as writable.
  virtual rocksdb::Status ReuseWritableFile(const std::string& fname, const std::string& old_fname,
                                            std::unique_ptr<rocksdb::WritableFile>* result,
                                            const rocksdb::EnvOptions& options) override {
    result->reset();
    if (options.use_mmap_writes) {
      return rocksdb::Status::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<rocksdb::WritableFile> underlying;
    rocksdb::Status status =
        rocksdb::EnvWrapper::ReuseWritableFile(fname, old_fname, &underlying, options);
    if (!status.ok()) {
      return status;
    }

    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    status = provider_->CreateCipherStream(env_level_, fname, true /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::WritableFile>(
        new EncryptedWritableFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
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
    status = provider_->CreateCipherStream(env_level_, fname, isNewFile /* new_file */, &stream);
    if (!status.ok()) {
      return status;
    }
    (*result) = std::unique_ptr<rocksdb::RandomRWFile>(
        new EncryptedRandomRWFile(underlying.release(), stream.release()));
    return rocksdb::Status::OK();
  }

  virtual rocksdb::Status DeleteFile(const std::string& fname) override {
    auto status = provider_->DeleteFile(fname);
    if (!status.ok()) {
      return status;
    }
    return EnvWrapper::DeleteFile(fname);
  }
  virtual rocksdb::Status RenameFile(const std::string& src, const std::string& target) override {
    auto status = provider_->RenameFile(src, target);
    if (!status.ok()) {
      return status;
    }
    return EnvWrapper::RenameFile(src, target);
  }

  virtual rocksdb::Status LinkFile(const std::string& src, const std::string& target) override {
    auto status = provider_->LinkFile(src, target);
    if (!status.ok()) {
      return status;
    }
    return EnvWrapper::LinkFile(src, target);
  }

 private:
  EncryptionProvider* provider_;
  enginepb::EnvLevel env_level_;
};

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
rocksdb::Env* NewEncryptedEnv(rocksdb::Env* base_env, EncryptionProvider* provider,
                              enginepb::EnvLevel env_level) {
  return new EncryptedEnv(base_env, provider, env_level);
}

// Encrypt one or more (partial) blocks of data at the file offset.
// Length of data is given in dataSize.
rocksdb::Status BlockAccessCipherStream::Encrypt(uint64_t fileOffset, char* data, size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

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
    auto status = EncryptBlock(blockIndex, block, (char*)scratch.data());
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
rocksdb::Status BlockAccessCipherStream::Decrypt(uint64_t fileOffset, char* data, size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

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
    auto status = DecryptBlock(blockIndex, block, (char*)scratch.data());
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
