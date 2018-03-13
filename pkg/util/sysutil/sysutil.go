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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sysutil re-exports symbols from package syscall that are known to be
// safe for cross-platform use. Using package syscall directly is forbidden.
package sysutil

import (
	"syscall"
)

// Signal is syscall.Signal. It implements os.Signal.
type Signal = syscall.Signal

// Errno is syscall.Errno.
type Errno = syscall.Errno

// WaitStatus is syscall.WaitStatus.
//
// NB: the docs suggest[1] that there are platform-independent types, but an
// inspection shows that at the time of writing, everything seems to return a
// syscall.WaitStatus.
//
// [1]: https://golang.org/pkg/os/#ProcessState.Sys
type WaitStatus = syscall.WaitStatus

// User and group ID functions. These always return -1 on Windows.
var (
	MaybeGetuid  = syscall.Getuid
	MaybeGeteuid = syscall.Geteuid
	MaybeGetgid  = syscall.Getgid
	MaybeGetegid = syscall.Getegid
)
