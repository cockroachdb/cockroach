// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <string>

/*
 * These provide various crypto primitives. They currently use CryptoPP.
 */

// HexString returns the lowercase hexadecimal representation of the data contained 's'.
// eg: HexString("1") -> "31" (hex(character value)), not "1" -> "1".
std::string HexString(const std::string& s);

// RandomBytes returns `length` bytes of data from a pseudo-random number generator.
// This is non-blocking.
// TODO(mberhault): it would be good to have a blocking version (/dev/random on *nix),
// but to do it properly we might want to pre-read in the background.
std::string RandomBytes(size_t length);
