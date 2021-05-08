// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import "github.com/cockroachdb/pebble/vfs"

// swappableFS is a vfs.FS that can be swapped out at a future time.
type swappableFS struct {
	vfs.FS
}

// set replaces the FS in a swappableFS.
func (s *swappableFS) set(fs vfs.FS) {
	s.FS = fs
}
