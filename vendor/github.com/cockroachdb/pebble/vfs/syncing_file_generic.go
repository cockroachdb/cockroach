// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !linux || arm
// +build !linux arm

package vfs

func (f *syncingFile) init() {
	f.syncTo = f.syncToGeneric
}

func (f *syncingFile) syncToGeneric(_ int64) error {
	return f.Sync()
}
