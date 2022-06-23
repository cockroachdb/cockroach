// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/intern"
)

const propertiesBlockRestartInterval = math.MaxInt32
const propGlobalSeqnumName = "rocksdb.external_sst_file.global_seqno"

var propTagMap = make(map[string]reflect.StructField)
var propBoolTrue = []byte{'1'}
var propBoolFalse = []byte{'0'}

var columnFamilyIDField = func() reflect.StructField {
	f, ok := reflect.TypeOf(Properties{}).FieldByName("ColumnFamilyID")
	if !ok {
		panic("Properties.ColumnFamilyID field not found")
	}
	return f
}()

var propOffsetTagMap = make(map[uintptr]string)

func init() {
	t := reflect.TypeOf(Properties{})
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("prop"); tag != "" {
			switch f.Type.Kind() {
			case reflect.Bool:
			case reflect.Uint32:
			case reflect.Uint64:
			case reflect.String:
			default:
				panic(fmt.Sprintf("unsupported property field type: %s %s", f.Name, f.Type))
			}
			propTagMap[tag] = f
			propOffsetTagMap[f.Offset] = tag
		}
	}
}

// Properties holds the sstable property values. The properties are
// automatically populated during sstable creation and load from the properties
// meta block when an sstable is opened.
type Properties struct {
	// ID of column family for this SST file, corresponding to the CF identified
	// by column_family_name.
	ColumnFamilyID uint64 `prop:"rocksdb.column.family.id"`
	// Name of the column family with which this SST file is associated. Empty if
	// the column family is unknown.
	ColumnFamilyName string `prop:"rocksdb.column.family.name"`
	// The name of the comparer used in this table.
	ComparerName string `prop:"rocksdb.comparator"`
	// The compression algorithm used to compress blocks.
	CompressionName string `prop:"rocksdb.compression"`
	// The compression options used to compress blocks.
	CompressionOptions string `prop:"rocksdb.compression_options"`
	// The time when the SST file was created. Since SST files are immutable,
	// this is equivalent to last modified time.
	CreationTime uint64 `prop:"rocksdb.creation.time"`
	// The total size of all data blocks.
	DataSize uint64 `prop:"rocksdb.data.size"`
	// The external sstable version format. Version 2 is the one RocksDB has been
	// using since 5.13. RocksDB only uses the global sequence number for an
	// sstable if this property has been set.
	ExternalFormatVersion uint32 `prop:"rocksdb.external_sst_file.version"`
	// Actual SST file creation time. 0 means unknown.
	FileCreationTime uint64 `prop:"rocksdb.file.creation.time"`
	// The name of the filter policy used in this table. Empty if no filter
	// policy is used.
	FilterPolicyName string `prop:"rocksdb.filter.policy"`
	// The size of filter block.
	FilterSize uint64 `prop:"rocksdb.filter.size"`
	// If 0, key is variable length. Otherwise number of bytes for each key.
	FixedKeyLen uint64 `prop:"rocksdb.fixed.key.length"`
	// Format version, reserved for backward compatibility.
	FormatVersion uint64 `prop:"rocksdb.format.version"`
	// The global sequence number to use for all entries in the table. Present if
	// the table was created externally and ingested whole.
	GlobalSeqNum uint64 `prop:"rocksdb.external_sst_file.global_seqno"`
	// Whether the index key is user key or an internal key.
	IndexKeyIsUserKey uint64 `prop:"rocksdb.index.key.is.user.key"`
	// Total number of index partitions if kTwoLevelIndexSearch is used.
	IndexPartitions uint64 `prop:"rocksdb.index.partitions"`
	// The size of index block.
	IndexSize uint64 `prop:"rocksdb.index.size"`
	// The index type. TODO(peter): add a more detailed description.
	IndexType uint32 `prop:"rocksdb.block.based.table.index.type"`
	// Whether delta encoding is used to encode the index values.
	IndexValueIsDeltaEncoded uint64 `prop:"rocksdb.index.value.is.delta.encoded"`
	// The name of the merger used in this table. Empty if no merger is used.
	MergerName string `prop:"rocksdb.merge.operator"`
	// The number of blocks in this table.
	NumDataBlocks uint64 `prop:"rocksdb.num.data.blocks"`
	// The number of deletion entries in this table, including both point and
	// range deletions.
	NumDeletions uint64 `prop:"rocksdb.deleted.keys"`
	// The number of entries in this table.
	NumEntries uint64 `prop:"rocksdb.num.entries"`
	// The number of merge operands in the table.
	NumMergeOperands uint64 `prop:"rocksdb.merge.operands"`
	// The number of range deletions in this table.
	NumRangeDeletions uint64 `prop:"rocksdb.num.range-deletions"`
	// The number of RANGEKEYDELs in this table.
	NumRangeKeyDels uint64 `prop:"pebble.num.range-key-dels"`
	// The number of RANGEKEYSETs in this table.
	NumRangeKeySets uint64 `prop:"pebble.num.range-key-sets"`
	// The number of RANGEKEYUNSETs in this table.
	NumRangeKeyUnsets uint64 `prop:"pebble.num.range-key-unsets"`
	// Timestamp of the earliest key. 0 if unknown.
	OldestKeyTime uint64 `prop:"rocksdb.oldest.key.time"`
	// The name of the prefix extractor used in this table. Empty if no prefix
	// extractor is used.
	PrefixExtractorName string `prop:"rocksdb.prefix.extractor.name"`
	// If filtering is enabled, was the filter created on the key prefix.
	PrefixFiltering bool `prop:"rocksdb.block.based.table.prefix.filtering"`
	// A comma separated list of names of the property collectors used in this
	// table.
	PropertyCollectorNames string `prop:"rocksdb.property.collectors"`
	// Total raw key size.
	RawKeySize uint64 `prop:"rocksdb.raw.key.size"`
	// Total raw rangekey key size.
	RawRangeKeyKeySize uint64 `prop:"pebble.raw.range-key.key.size"`
	// Total raw rangekey value size.
	RawRangeKeyValueSize uint64 `prop:"pebble.raw.range-key.value.size"`
	// Total raw value size.
	RawValueSize uint64 `prop:"rocksdb.raw.value.size"`
	// Size of the top-level index if kTwoLevelIndexSearch is used.
	TopLevelIndexSize uint64 `prop:"rocksdb.top-level.index.size"`
	// User collected properties.
	UserProperties map[string]string
	// If filtering is enabled, was the filter created on the whole key.
	WholeKeyFiltering bool `prop:"rocksdb.block.based.table.whole.key.filtering"`

	// Loaded set indicating which fields have been loaded from disk. Indexed by
	// the field's byte offset within the struct
	// (reflect.StructField.Offset). Only set if the properties have been loaded
	// from a file. Only exported for testing purposes.
	Loaded map[uintptr]struct{}
}

// NumPointDeletions returns the number of point deletions in this table.
func (p *Properties) NumPointDeletions() uint64 {
	return p.NumDeletions - p.NumRangeDeletions
}

// NumRangeKeys returns a count of the number of range keys in this table.
func (p *Properties) NumRangeKeys() uint64 {
	return p.NumRangeKeyDels + p.NumRangeKeySets + p.NumRangeKeyUnsets
}

func (p *Properties) String() string {
	var buf bytes.Buffer
	v := reflect.ValueOf(*p)
	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		ft := vt.Field(i)
		tag := ft.Tag.Get("prop")
		if tag == "" {
			continue
		}

		f := v.Field(i)
		// TODO(peter): Use f.IsZero() when we can rely on go1.13.
		if zero := reflect.Zero(f.Type()); zero.Interface() == f.Interface() {
			// Skip printing of zero values which were not loaded from disk.
			if _, ok := p.Loaded[ft.Offset]; !ok {
				continue
			}
		}

		fmt.Fprintf(&buf, "%s: ", tag)
		switch ft.Type.Kind() {
		case reflect.Bool:
			fmt.Fprintf(&buf, "%t\n", f.Bool())
		case reflect.Uint32:
			fmt.Fprintf(&buf, "%d\n", f.Uint())
		case reflect.Uint64:
			u := f.Uint()
			if ft.Offset == columnFamilyIDField.Offset && u == math.MaxInt32 {
				fmt.Fprintf(&buf, "-\n")
			} else {
				fmt.Fprintf(&buf, "%d\n", f.Uint())
			}
		case reflect.String:
			fmt.Fprintf(&buf, "%s\n", f.String())
		default:
			panic("not reached")
		}
	}
	keys := make([]string, 0, len(p.UserProperties))
	for key := range p.UserProperties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&buf, "%s: %s\n", key, p.UserProperties[key])
	}
	return buf.String()
}

func (p *Properties) load(b block, blockOffset uint64) error {
	i, err := newRawBlockIter(bytes.Compare, b)
	if err != nil {
		return err
	}
	p.Loaded = make(map[uintptr]struct{})
	v := reflect.ValueOf(p).Elem()
	for valid := i.First(); valid; valid = i.Next() {
		tag := intern.Bytes(i.Key().UserKey)
		if f, ok := propTagMap[tag]; ok {
			p.Loaded[f.Offset] = struct{}{}
			field := v.FieldByIndex(f.Index)
			switch f.Type.Kind() {
			case reflect.Bool:
				field.SetBool(bytes.Equal(i.Value(), propBoolTrue))
			case reflect.Uint32:
				field.SetUint(uint64(binary.LittleEndian.Uint32(i.Value())))
			case reflect.Uint64:
				var n uint64
				if tag == propGlobalSeqnumName {
					n = binary.LittleEndian.Uint64(i.Value())
				} else {
					n, _ = binary.Uvarint(i.Value())
				}
				field.SetUint(n)
			case reflect.String:
				field.SetString(intern.Bytes(i.Value()))
			default:
				panic("not reached")
			}
			continue
		}
		if p.UserProperties == nil {
			p.UserProperties = make(map[string]string)
		}
		p.UserProperties[tag] = string(i.Value())
	}
	return nil
}

func (p *Properties) saveBool(m map[string][]byte, offset uintptr, value bool) {
	tag := propOffsetTagMap[offset]
	if value {
		m[tag] = propBoolTrue
	} else {
		m[tag] = propBoolFalse
	}
}

func (p *Properties) saveUint32(m map[string][]byte, offset uintptr, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	m[propOffsetTagMap[offset]] = buf[:]
}

func (p *Properties) saveUint64(m map[string][]byte, offset uintptr, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	m[propOffsetTagMap[offset]] = buf[:]
}

func (p *Properties) saveUvarint(m map[string][]byte, offset uintptr, value uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], value)
	m[propOffsetTagMap[offset]] = buf[:n]
}

func (p *Properties) saveString(m map[string][]byte, offset uintptr, value string) {
	m[propOffsetTagMap[offset]] = []byte(value)
}

func (p *Properties) save(w *rawBlockWriter) {
	m := make(map[string][]byte)
	for k, v := range p.UserProperties {
		m[k] = []byte(v)
	}

	p.saveUvarint(m, unsafe.Offsetof(p.ColumnFamilyID), p.ColumnFamilyID)
	if p.ColumnFamilyName != "" {
		p.saveString(m, unsafe.Offsetof(p.ColumnFamilyName), p.ColumnFamilyName)
	}
	if p.ComparerName != "" {
		p.saveString(m, unsafe.Offsetof(p.ComparerName), p.ComparerName)
	}
	if p.CompressionName != "" {
		p.saveString(m, unsafe.Offsetof(p.CompressionName), p.CompressionName)
	}
	if p.CompressionOptions != "" {
		p.saveString(m, unsafe.Offsetof(p.CompressionOptions), p.CompressionOptions)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.CreationTime), p.CreationTime)
	p.saveUvarint(m, unsafe.Offsetof(p.DataSize), p.DataSize)
	if p.ExternalFormatVersion != 0 {
		p.saveUint32(m, unsafe.Offsetof(p.ExternalFormatVersion), p.ExternalFormatVersion)
		p.saveUint64(m, unsafe.Offsetof(p.GlobalSeqNum), p.GlobalSeqNum)
	}
	if p.FileCreationTime > 0 {
		p.saveUvarint(m, unsafe.Offsetof(p.FileCreationTime), p.FileCreationTime)
	}
	if p.FilterPolicyName != "" {
		p.saveString(m, unsafe.Offsetof(p.FilterPolicyName), p.FilterPolicyName)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.FilterSize), p.FilterSize)
	p.saveUvarint(m, unsafe.Offsetof(p.FixedKeyLen), p.FixedKeyLen)
	p.saveUvarint(m, unsafe.Offsetof(p.FormatVersion), p.FormatVersion)
	p.saveUvarint(m, unsafe.Offsetof(p.IndexKeyIsUserKey), p.IndexKeyIsUserKey)
	if p.IndexPartitions != 0 {
		p.saveUvarint(m, unsafe.Offsetof(p.IndexPartitions), p.IndexPartitions)
		p.saveUvarint(m, unsafe.Offsetof(p.TopLevelIndexSize), p.TopLevelIndexSize)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.IndexSize), p.IndexSize)
	p.saveUint32(m, unsafe.Offsetof(p.IndexType), p.IndexType)
	p.saveUvarint(m, unsafe.Offsetof(p.IndexValueIsDeltaEncoded), p.IndexValueIsDeltaEncoded)
	if p.MergerName != "" {
		p.saveString(m, unsafe.Offsetof(p.MergerName), p.MergerName)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.NumDataBlocks), p.NumDataBlocks)
	p.saveUvarint(m, unsafe.Offsetof(p.NumEntries), p.NumEntries)
	p.saveUvarint(m, unsafe.Offsetof(p.NumDeletions), p.NumDeletions)
	p.saveUvarint(m, unsafe.Offsetof(p.NumMergeOperands), p.NumMergeOperands)
	p.saveUvarint(m, unsafe.Offsetof(p.NumRangeDeletions), p.NumRangeDeletions)
	if p.NumRangeKeys() > 0 {
		p.saveUvarint(m, unsafe.Offsetof(p.NumRangeKeyDels), p.NumRangeKeyDels)
		p.saveUvarint(m, unsafe.Offsetof(p.NumRangeKeySets), p.NumRangeKeySets)
		p.saveUvarint(m, unsafe.Offsetof(p.NumRangeKeyUnsets), p.NumRangeKeyUnsets)
		p.saveUvarint(m, unsafe.Offsetof(p.RawRangeKeyKeySize), p.RawRangeKeyKeySize)
		p.saveUvarint(m, unsafe.Offsetof(p.RawRangeKeyValueSize), p.RawRangeKeyValueSize)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.OldestKeyTime), p.OldestKeyTime)
	if p.PrefixExtractorName != "" {
		p.saveString(m, unsafe.Offsetof(p.PrefixExtractorName), p.PrefixExtractorName)
	}
	p.saveBool(m, unsafe.Offsetof(p.PrefixFiltering), p.PrefixFiltering)
	if p.PropertyCollectorNames != "" {
		p.saveString(m, unsafe.Offsetof(p.PropertyCollectorNames), p.PropertyCollectorNames)
	}
	p.saveUvarint(m, unsafe.Offsetof(p.RawKeySize), p.RawKeySize)
	p.saveUvarint(m, unsafe.Offsetof(p.RawValueSize), p.RawValueSize)
	p.saveBool(m, unsafe.Offsetof(p.WholeKeyFiltering), p.WholeKeyFiltering)

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		w.add(InternalKey{UserKey: []byte(key)}, m[key])
	}
}
