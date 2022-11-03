// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "github.com/cockroachdb/cockroach/pkg/config/zonepb"

// ZoneConfigWithRawBytes wraps a zone config together with its expected
// raw bytes. For cached modified zone configs, the raw bytes should be the
// deserialized bytes from the zone config proto. Though, the raw bytes should
// be the raw value read from kv when it's first read from storage.
type ZoneConfigWithRawBytes struct {
	zc *zonepb.ZoneConfig
	rb []byte
}

// NewZoneConfigWithRawBytes creates a new ZoneConfigWithRawBytes.
func NewZoneConfigWithRawBytes(zc *zonepb.ZoneConfig, rb []byte) *ZoneConfigWithRawBytes {
	return &ZoneConfigWithRawBytes{zc: zc, rb: rb}
}

// Size returns the size of data represented by this struct not the actual size
// of this struct.
func (zc *ZoneConfigWithRawBytes) Size() int {
	return zc.zc.Size() + len(zc.rb)
}

// ZoneConfig returns the underlying zone config.
func (zc *ZoneConfigWithRawBytes) ZoneConfig() *zonepb.ZoneConfig {
	return zc.zc
}

// RawBytes returns the raw bytes.
func (zc *ZoneConfigWithRawBytes) RawBytes() []byte {
	return zc.rb
}
