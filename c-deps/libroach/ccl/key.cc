// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "key.h"
#include <filters.h>  // CryptoPP
#include <hex.h>      // CryptoPP
#include <sha.h>      // CryptoPP

std::string KeyHash(const std::string& k) {
  std::string value;
  CryptoPP::SHA256 hash;

  CryptoPP::StringSource ss(
      k, true /* PumpAll */,
      new CryptoPP::HashFilter(hash, new CryptoPP::HexEncoder(new CryptoPP::StringSink(value), false /* uppercase */)));

  return value;
}
