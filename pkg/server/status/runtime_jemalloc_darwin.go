// Copyright 2018 The Cockroach Authors.
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

// +build !stdmalloc,darwin

package status

// extern void je_zone_register();
import "C"

func init() {
	// On macOS, je_zone_register is run at init time to register jemalloc with
	// the system allocator. Unfortunately, all the machinery for this is in a
	// single file, and is not referenced elsewhere, causing the linker to omit
	// the file's symbols. Manually force the presence of these symbols on macOS
	// by explicitly calling this method.
	//
	// Note that the same could be achieved via linker flags, but go >= 1.9.4
	// requires explicit opt-in for the required flag, which we want to avoid
	// having to put up with.
	//
	// See https://github.com/jemalloc/jemalloc/issues/708 and the references
	// within.
	C.je_zone_register()
}
