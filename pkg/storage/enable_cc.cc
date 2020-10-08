// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Since we link against C++ libraries, like RocksDB and libroach, we need to
// link against the C++ standard library. This presence of this file convinces
// cgo to link this package using the C++ compiler instead of the C compiler,
// which brings in the appropriate, platform-specific C++ library (e.g., libc++
// on macOS or libstdc++ on Linux).
