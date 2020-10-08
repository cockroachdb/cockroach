// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <thread>
#include "../file_registry.h"
#include "../fmt.h"
#include "../testutils.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "ctr_stream.h"
#include "testutils.h"

using namespace cockroach;
using namespace testutils;

TEST(EncryptedEnv, ConcurrentAccess) {
  // This test creates a standalone encrypted env, verifies that what goes
  // in comes out, and that concurrent accesses to the same file handle are ok.
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  auto key_manager = new MemKeyManager(MakeAES128Key(env.get()));
  auto stream = new CTRCipherStreamCreator(key_manager, enginepb::Data);

  auto file_registry =
      std::unique_ptr<FileRegistry>(new FileRegistry(env.get(), "/", false /* read-only */));
  EXPECT_OK(file_registry->Load());

  std::unique_ptr<rocksdb::Env> encrypted_env(
      rocksdb_utils::NewEncryptedEnv(env.get(), file_registry.get(), stream));

  std::string filename("/foo");
  std::string contents("this is the string stored inside the file!");
  size_t kContentsLength = 42;
  ASSERT_EQ(kContentsLength, contents.size());

  // Write the file.
  EXPECT_OK(
      rocksdb::WriteStringToFile(encrypted_env.get(), contents, filename, false /* should_sync */));

  // Read the file using the mem env (no encryption).
  std::string result_plain;
  EXPECT_OK(rocksdb::ReadFileToString(env.get(), filename, &result_plain));
  EXPECT_STRNE(contents.c_str(), result_plain.c_str());

  // Read the file back using the encrypted env.
  std::string result_encrypted;
  EXPECT_OK(rocksdb::ReadFileToString(encrypted_env.get(), filename, &result_encrypted));
  EXPECT_STREQ(contents.c_str(), result_encrypted.c_str());

  // Open as a random access file.
  std::unique_ptr<rocksdb::RandomAccessFile> file;
  EXPECT_OK(encrypted_env->NewRandomAccessFile(filename, &file, rocksdb::EnvOptions()));

  // Reader thread. Captures all useful variables.
  auto read_file = [&]() {
    char scratch[kContentsLength];  // needs to be at least len(contents).
    rocksdb::Slice result_read;

    for (int i = 0; i < 100; i++) {
      EXPECT_OK(file->Read(0, kContentsLength, &result_read, scratch));
      EXPECT_EQ(kContentsLength, result_read.size());

      // We need to go through Slice.ToString as .data() does not have a null terminator.
      EXPECT_STREQ(contents.c_str(), result_read.ToString().c_str());
    }
  };

  // Call it once by itself.
  read_file();

  // Run two at the same time. We're not using rocksdb thread utilities as they don't support
  // lambda functions with variable capture, everything has to done through args.
  auto t1 = std::thread(read_file);
  auto t2 = std::thread(read_file);
  t1.join();
  t2.join();
}

namespace {

rocksdb::Status checkFileEntry(FileRegistry& registry, const std::string& filename,
                               enginepbccl::EncryptionType enc_type) {
  auto entry = registry.GetFileEntry(filename);
  if (entry == nullptr) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s has no entry", filename.c_str()));
  }

  enginepbccl::EncryptionSettings enc_settings;
  if (!enc_settings.ParseFromString(entry->encryption_settings())) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("failed to parse encryption settings for file %s", filename.c_str()));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status checkNoFileEntry(FileRegistry& registry, const std::string& filename) {
  auto entry = registry.GetFileEntry(filename);
  if (entry != nullptr) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s has an unexpected entry", filename.c_str()));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status checkFileContents(rocksdb::Env* env, const std::string& filename,
                                  const std::string& contents) {
  std::string result;
  auto status = rocksdb::ReadFileToString(env, filename, &result);
  if (!status.ok()) {
    return status;
  }

  if (contents != result) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s contents mismatch, expected %s, got %s", filename.c_str(),
                          contents.c_str(), result.c_str()));
  }

  return rocksdb::Status::OK();
}

};  // anonymous namespace

TEST(EncryptedEnv, BasicOps) {
  // Check various file operations against an encrypted env.
  // This exercises the env/file registry interaction.
  // We need to use a real underlying env as the MemEnv takes a **lot**
  // of shortcuts.
  rocksdb::Env* env = rocksdb::Env::Default();
  auto key_manager = new MemKeyManager(MakeAES128Key(env));
  auto stream = new CTRCipherStreamCreator(key_manager, enginepb::Data);

  auto tmpdir = std::unique_ptr<TempDirHandler>(new TempDirHandler());

  auto file_registry =
      std::unique_ptr<FileRegistry>(new FileRegistry(env, tmpdir->Path(""), false /* read-only */));
  EXPECT_OK(file_registry->Load());

  std::unique_ptr<rocksdb::Env> encrypted_env(
      rocksdb_utils::NewEncryptedEnv(env, file_registry.get(), stream));

  auto file1 = tmpdir->Path("foo1");
  auto file2 = tmpdir->Path("foo2");
  auto file3 = tmpdir->Path("foo3");

  std::string contents("this is the first file!");
  std::string contents2("this is the second file!");
  ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents, file1, false));

  // Check its presence in the file registry.
  EXPECT_OK(checkFileEntry(*file_registry, file1, enginepbccl::AES128_CTR));

  // Rename file.
  ASSERT_OK(encrypted_env->RenameFile(file1, file2));
  EXPECT_OK(checkNoFileEntry(*file_registry, file1));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));

  // Link file (hard link).
  ASSERT_OK(encrypted_env->LinkFile(file2, file3));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileEntry(*file_registry, file3, enginepbccl::AES128_CTR));

  // Delete file.
  ASSERT_OK(encrypted_env->DeleteFile(file2));
  EXPECT_OK(checkNoFileEntry(*file_registry, file2));
  EXPECT_OK(checkFileEntry(*file_registry, file3, enginepbccl::AES128_CTR));

  ASSERT_OK(encrypted_env->DeleteFile(file3));
  EXPECT_OK(checkNoFileEntry(*file_registry, file2));
  EXPECT_OK(checkNoFileEntry(*file_registry, file3));

  /***** Odd cases *****/
  ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents, file1, false));
  ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents2, file2, false));
  EXPECT_OK(checkFileEntry(*file_registry, file1, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file1, contents));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file2, contents2));

  // Rename to existing file (replace).
  ASSERT_OK(encrypted_env->RenameFile(file1, file2));
  EXPECT_OK(checkNoFileEntry(*file_registry, file1));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file2, contents));

  ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents2, file3, false));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file3, contents2));

  // Link to an existing file: fails with "file exists".
  ASSERT_ERR(encrypted_env->LinkFile(file2, file3), ".* File exists");
  EXPECT_OK(checkFileContents(encrypted_env.get(), file2, contents));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file3, contents2));

  // Let's check that the plain env can't read the contents.
  EXPECT_PARTIAL_ERR(checkFileContents(env, file2, contents),
                     "file .*foo2 contents mismatch, expected this is the first file!, got ");

  /***** Switch to plaintext *****/
  key_manager->set_key(nullptr);

  // Rename.
  ASSERT_OK(encrypted_env->RenameFile(file2, file1));
  EXPECT_OK(checkNoFileEntry(*file_registry, file2));
  EXPECT_OK(checkFileEntry(*file_registry, file1, enginepbccl::AES128_CTR));

  // Link.
  ASSERT_OK(encrypted_env->LinkFile(file1, file2));
  EXPECT_OK(checkFileEntry(*file_registry, file1, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));

  // Create a new file. This should overwrite the previous file entry.
  std::string contents3("we're in plaintext!");
  ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents3, file1, false));
  EXPECT_OK(checkFileEntry(*file_registry, file1, enginepbccl::Plaintext));
  EXPECT_OK(checkFileEntry(*file_registry, file2, enginepbccl::AES128_CTR));
  EXPECT_OK(checkFileContents(encrypted_env.get(), file1, contents3));
  // Check with the plain env.
  EXPECT_OK(checkFileContents(env, file1, contents3));
  EXPECT_OK(checkFileContents(env, file2, contents3));
}

TEST(EncryptedEnv, ReuseWritableFileUnderlyingFailure) {
  // Ensure all WALs are either empty or decryptable no matter when/whether
  // `EncryptedEnv::ReuseWritableFile` fails. See comment above
  // `EncryptedEnv::ReuseWritableFile`'s definition for details on why this
  // guarantee is necessary for crash-safety.

  // `ReuseWritableFileInjectionEnv` is used as the env underlying an
  // `EncryptedEnv`. It injects faults in the operations used by
  // `EncryptedEnv::ReuseWritableFile()`.
  class ReuseWritableFileInjectionEnv : public rocksdb::EnvWrapper {
   public:
    enum class Mode {
      kNone,
      kFailCreation,
      kFailDeletion,
      kEnd,
    };

    explicit ReuseWritableFileInjectionEnv(Env* target) : EnvWrapper(target), mode_(Mode::kNone) {}

    void set_mode(Mode mode) { mode_ = mode; }

    rocksdb::Status NewWritableFile(const std::string& fname,
                                    std::unique_ptr<rocksdb::WritableFile>* result,
                                    const rocksdb::EnvOptions& options) override {
      if (mode_ == Mode::kFailCreation) {
        return rocksdb::Status::IOError("injected error");
      }
      return rocksdb::EnvWrapper::NewWritableFile(fname, result, options);
    }

    rocksdb::Status DeleteFile(const std::string& fname) override {
      if (mode_ == Mode::kFailDeletion) {
        return rocksdb::Status::IOError("injected error");
      }
      return rocksdb::EnvWrapper::DeleteFile(fname);
    }

   private:
    Mode mode_;
  };

  for (int i = 0; i < static_cast<int>(ReuseWritableFileInjectionEnv::Mode::kEnd); ++i) {
    auto mode = static_cast<ReuseWritableFileInjectionEnv::Mode>(i);
    ReuseWritableFileInjectionEnv env(rocksdb::Env::Default());
    auto key_manager = new MemKeyManager(MakeAES128Key(&env));
    auto stream = new CTRCipherStreamCreator(key_manager, enginepb::Data);

    auto tmpdir = std::unique_ptr<TempDirHandler>(new TempDirHandler());

    auto file_registry = std::unique_ptr<FileRegistry>(
        new FileRegistry(&env, tmpdir->Path(""), false /* read-only */));
    EXPECT_OK(file_registry->Load());

    std::unique_ptr<rocksdb::Env> encrypted_env(
        rocksdb_utils::NewEncryptedEnv(&env, file_registry.get(), stream));

    auto old_file = tmpdir->Path("foo1");
    std::string contents("this is a file!");
    ASSERT_OK(rocksdb::WriteStringToFile(encrypted_env.get(), contents, old_file, false));

    auto new_file = tmpdir->Path("foo2");
    env.set_mode(mode);
    {
      std::unique_ptr<rocksdb::WritableFile> res;
      if (mode == ReuseWritableFileInjectionEnv::Mode::kNone) {
        EXPECT_OK(
            encrypted_env->ReuseWritableFile(new_file, old_file, &res, rocksdb::EnvOptions()));
      } else {
        EXPECT_TRUE(
            encrypted_env->ReuseWritableFile(new_file, old_file, &res, rocksdb::EnvOptions())
                .IsIOError());
      }
    }

    switch (mode) {
    case ReuseWritableFileInjectionEnv::Mode::kNone: {
      // In success scenario, the new file should be empty and the old file should be deleted.
      std::string new_file_contents;
      EXPECT_OK(rocksdb::ReadFileToString(encrypted_env.get(), new_file, &new_file_contents));
      EXPECT_STREQ("", new_file_contents.c_str());
      EXPECT_TRUE(encrypted_env->FileExists(old_file).IsNotFound());
      break;
    }
    case ReuseWritableFileInjectionEnv::Mode::kFailCreation:
    case ReuseWritableFileInjectionEnv::Mode::kFailDeletion: {
      // In failure scenarios, all existing files must be empty or readable.
      for (const auto& file : {old_file, new_file}) {
        auto exists_status = encrypted_env->FileExists(file);
        if (exists_status.ok()) {
          std::string file_contents;
          rocksdb::ReadFileToString(encrypted_env.get(), file, &file_contents);
          if (!file_contents.empty()) {
            EXPECT_STREQ(contents.c_str(), file_contents.c_str());
          }
        } else {
          EXPECT_TRUE(exists_status.IsNotFound());
        }
      }
      break;
    }
    case ReuseWritableFileInjectionEnv::Mode::kEnd:
      assert(false);
      break;
    }
  }
}
