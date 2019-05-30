// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Since we link against C++ libraries, like RocksDB and libroach, we need to
// link against the C++ standard library. This presence of this file convinces
// cgo to link this package using the C++ compiler instead of the C compiler,
// which brings in the appropriate, platform-specific C++ library (e.g., libc++
// on macOS or libstdc++ on Linux).
