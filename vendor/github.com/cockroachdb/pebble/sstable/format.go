// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// TableFormat specifies the format version for sstables. The legacy LevelDB
// format is format version 1.
type TableFormat uint32

// The available table formats, representing the tuple (magic number, version
// number). Note that these values are not (and should not) be serialized to
// disk. The ordering should follow the order the versions were introduced to
// Pebble (i.e. the history is linear).
const (
	TableFormatUnspecified TableFormat = iota
	TableFormatLevelDB
	TableFormatRocksDBv2
	TableFormatPebblev1 // Block properties.
	TableFormatPebblev2 // Range keys.

	TableFormatMax = TableFormatPebblev2
)

// ParseTableFormat parses the given magic bytes and version into its
// corresponding internal TableFormat.
func ParseTableFormat(magic []byte, version uint32) (TableFormat, error) {
	switch string(magic) {
	case levelDBMagic:
		return TableFormatLevelDB, nil
	case rocksDBMagic:
		if version != rocksDBFormatVersion2 {
			return TableFormatUnspecified, base.CorruptionErrorf(
				"pebble/table: unsupported rocksdb format version %d", errors.Safe(version),
			)
		}
		return TableFormatRocksDBv2, nil
	case pebbleDBMagic:
		switch version {
		case 1:
			return TableFormatPebblev1, nil
		case 2:
			return TableFormatPebblev2, nil
		default:
			return TableFormatUnspecified, base.CorruptionErrorf(
				"pebble/table: unsupported pebble format version %d", errors.Safe(version),
			)
		}
	default:
		return TableFormatUnspecified, base.CorruptionErrorf(
			"pebble/table: invalid table (bad magic number)",
		)
	}
}

// AsTuple returns the TableFormat's (Magic String, Version) tuple.
func (f TableFormat) AsTuple() (string, uint32) {
	switch f {
	case TableFormatLevelDB:
		return levelDBMagic, 0
	case TableFormatRocksDBv2:
		return rocksDBMagic, 2
	case TableFormatPebblev1:
		return pebbleDBMagic, 1
	case TableFormatPebblev2:
		return pebbleDBMagic, 2
	default:
		panic("sstable: unknown table format version tuple")
	}
}

// String returns the TableFormat (Magic String,Version) tuple.
func (f TableFormat) String() string {
	switch f {
	case TableFormatLevelDB:
		return "(LevelDB)"
	case TableFormatRocksDBv2:
		return "(RocksDB,v2)"
	case TableFormatPebblev1:
		return "(Pebble,v1)"
	case TableFormatPebblev2:
		return "(Pebble,v2)"
	default:
		panic("sstable: unknown table format version tuple")
	}
}
