// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package growstack

// Grow grows the goroutine stack by 16 KB. Goroutine stacks currently start
// at 2 KB in size. The code paths through the storage package often need a
// stack that is 32 KB in size. The stack growth is mildly expensive making it
// useful to trick the runtime into growing the stack early. Since goroutine
// stacks grow in multiples of 2 and start at 2 KB in size, by placing a 16 KB
// object on the stack early in the lifetime of a goroutine we force the
// runtime to use a 32 KB stack for the goroutine.
func Grow()
