// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package growstack

// Grow grows the goroutine stack by 32 KB. Goroutine stacks currently start
// at 2 KB in size. The code paths through the kvserver package often need a
// stack that is at least 32 KB in size. The stack growth is mildly expensive
// making it useful to trick the runtime into growing the stack early. Since
// goroutine stacks grow in multiples of 2 and start at 2 KB in size, by
// placing a 32 KB object on the stack early in the lifetime of a goroutine we
// force the runtime to use a 64 KB stack for the goroutine.
func Grow()
