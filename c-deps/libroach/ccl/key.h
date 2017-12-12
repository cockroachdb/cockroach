// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <config.h>  // CryptoPP
#include <iostream>
#include <string>

typedef std::string KeyID;
typedef std::string RawKey;

enum EncryptionType { PLAIN, AES };

// EncryptionKey represents a key: type (PLAIN or AES) and actual key contents.
// TODO(mberhault): we would like to be able to mlock and madvise(MADV_DONTDUMP) the
// memory handling keys. This includes the permanent EncryptionKey and any temporary
// buffers (eg: when reading files).
struct EncryptionKey {
  EncryptionType type;
  // The Key ID is a sha-256 hash of the key contents. It is lowercase, without groupings or terminator.
  // **WARNING**: the key ID is persisted to disk, do not change the format.
  KeyID id;
  RawKey key;
  // source describes the source of the key. This could be a filename or
  // simply a description of the generator (eg: "data key manager").
  std::string source;
};

// KeyHash returns the sha-256 hash of the string. Returned value is in hexadecimal format.
std::string KeyHash(const std::string& k);
