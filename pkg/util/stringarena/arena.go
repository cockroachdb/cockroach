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

package stringarena

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// Arena provided arena allocation of a string from a []byte, reducing GC
// pressure significantly if a large number of strings with a similar lifetime
// are being created.
type Arena struct {
	alloc []byte
	acc   *mon.BoundAccount
}

// Make creates a new Arena with the specified monitoring account. If acc is
// nil, memory monitoring will be disabled.
func Make(acc *mon.BoundAccount) Arena {
	return Arena{acc: acc}
}

// AllocBytes allocates a string in the arena with contents specified by
// b. Returns an error on memory accounting failure.
func (a *Arena) AllocBytes(ctx context.Context, b []byte) (string, error) {
	n := len(b)
	if cap(a.alloc)-len(a.alloc) < n {
		if err := a.reserve(ctx, n); err != nil {
			return "", err
		}
	}

	pos := len(a.alloc)
	data := a.alloc[pos : pos+n : pos+n]
	a.alloc = a.alloc[:pos+n]

	copy(data, b)
	// NB: string and []byte have the same layout for the first 2 fields. This
	// use of unsafe avoids an allocation. We're promising to never mutate "data"
	// at this point, which is true because we carved it off from ss.alloc.
	return *(*string)(unsafe.Pointer(&data)), nil
}

// TODO(peter): This is copied from util/bufalloc in order to support memory
// accounting. Perhaps figure out how to share the code.
func (a *Arena) reserve(ctx context.Context, n int) error {
	const chunkAllocMinSize = 512
	const chunkAllocMaxSize = 16384

	allocSize := cap(a.alloc) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	if a.acc != nil {
		if err := a.acc.Grow(ctx, int64(allocSize)); err != nil {
			return err
		}
	}
	a.alloc = make([]byte, 0, allocSize)
	return nil
}
