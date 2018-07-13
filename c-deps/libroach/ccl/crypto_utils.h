// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <rocksdb/status.h>
#include <string>
#include "../rocksdbutils/env_encryption.h"
#include "ccl/storageccl/engineccl/enginepbccl/key_registry.pb.h"

namespace enginepbccl = cockroach::ccl::storageccl::engineccl::enginepbccl;

/*
 * These provide various crypto primitives. They currently use CryptoPP.
 */

// HexString returns the lowercase hexadecimal representation of the data contained 's'.
// eg: HexString("1") -> "31" (hex(character value)), not "1" -> "1".
std::string HexString(const std::string& s);

// RandomBytes returns `length` bytes of data from a pseudo-random number generator.
// This is non-blocking.
std::string RandomBytes(size_t length);

// Create a new AES cipher using the passed-in key.
// Suitable for encryption only, Decrypt is not implemented.
rocksdb_utils::BlockCipher* NewAESEncryptCipher(const enginepbccl::SecretKey* key);

// Returns true if CryptoPP is using AES-NI.
bool UsesAESNI();

// DisableCoreFile sets the maximum size of a core file to 0. Returns success
// if successfully called.
rocksdb::Status DisableCoreFile();
