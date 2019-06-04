// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build gc,go1.9

package engine

import "unsafe"

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the mallocgc function. Note that this
// access is necessarily tied to a specific Go release which is why this file
// is protected by a build tag.

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer
