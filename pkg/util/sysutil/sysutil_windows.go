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

package sysutil

import (
	"syscall"
)

// Keep this file in sync with sysutil_unix.go! Every exported symbol in this
// file must have a counterpart in sysutil_unix.go.

// Best-effort signal definitions.
//
// These constants may refer to an unsupported signal on the current platform.
// Passing an unsupported signal to signal.Notify is safe, provided your code
// will function correctly if it never receives a signal of that type. If
// failing to compile in the presence of an unsupported signal is preferred,
// refer to golang.org/x/sys/unix.SIGNAL directly.
const (
	MaybeSIGHUP  = syscall.Signal(0)
	MaybeSIGQUIT = syscall.Signal(0)
	MaybeSIGTERM = syscall.Signal(0)
	MaybeSIGUSR1 = syscall.Signal(0)
	MaybeSIGUSR2 = syscall.Signal(0)
)
