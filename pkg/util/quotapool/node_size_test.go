// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package quotapool

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestNodeSize ensures that the byte size of a node matches the expectation.
func TestNodeSize(t *testing.T) {
	assert.Equal(t, 32+8*bufferSize, int(unsafe.Sizeof(node{})))
}
