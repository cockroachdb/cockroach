// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package build

// const char* compilerVersion() {
// #if defined(__clang__)
// 	return __VERSION__;
// #elif defined(__GNUC__) || defined(__GNUG__)
// 	return "gcc " __VERSION__;
// #else
// 	return "non-gcc, non-clang (or an unrecognized version)";
// #endif
// }
import "C"

func cgoVersion() string {
	return C.GoString(C.compilerVersion())
}
