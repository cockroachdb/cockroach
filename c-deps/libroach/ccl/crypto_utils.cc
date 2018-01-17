// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "crypto_utils.h"
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>
#include <cryptopp/osrng.h>
#include <cryptopp/sha.h>

std::string HexString(const std::string& s) {
  std::string value;

  CryptoPP::StringSource ss(
      s, true /* PumpAll */,
      new CryptoPP::HexEncoder(new CryptoPP::StringSink(value), false /* uppercase */));

  return value;
}

std::string RandomBytes(size_t length) {
  CryptoPP::SecByteBlock data(length);
  CryptoPP::OS_GenerateRandomBlock(false /* blocking */, data, length);
  return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}
