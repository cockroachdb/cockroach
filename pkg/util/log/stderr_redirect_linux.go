// Copyright 2017 The Cockroach Authors.
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

package log

import (
	"syscall"
)

func dupFD2(oldfd uintptr, newfd uintptr) error {
	// NB: This uses Dup3 instead of Dup2 because newer architectures like
	// linux/arm64 do not include legacy syscalls like Dup2. Dup3 was introduced
	// in Kernel 2.6.27; at the time of writing, CockroachDB targets 2.6.32, so
	// we can assume Dup3 is available.
	//
	// See https://groups.google.com/forum/#!topic/golang-dev/zpeFtN2z5Fc.
	return syscall.Dup3(int(oldfd), int(newfd), 0)
}
