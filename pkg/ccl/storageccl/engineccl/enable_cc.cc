// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Since we link against C++ libraries, like RocksDB and libroach, we need to
// link against the C++ standard library. This presence of this file convinces
// cgo to link this package using the C++ compiler instead of the C compiler,
// which brings in the appropriate, platform-specific C++ library (e.g., libc++
// on macOS or libstdc++ on Linux).
