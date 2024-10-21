// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	// cockroachColRoachKey is a roachpb.Key user key. It does NOT include the
	// 0x00 terminator byte that a serialized engine key includes.
	cockroachColRoachKey int = iota
	// cockroachColMVCCWallTime is the wall time component of a MVCC timestamp,
	// or zero if not an MVCC key.
	cockroachColMVCCWallTime
	// cockroachColMVCCLogical is the logical time component of a MVCC
	// timestamp, or zero if not an MVCC key.
	cockroachColMVCCLogical
	// cockroachColUntypedVersion holds any non-empty, non-MVCC version. It does
	// NOT include the 0x00 separator byte that delimits the prefix and suffix
	// in a serialized engine key. In practice, this column is used to store the
	// version of lock-table keys.
	cockroachColUntypedVersion
	cockroachColCount
)

var keySchema = colblk.KeySchema{
	Name: "crdb1",
	ColumnTypes: []colblk.DataType{
		cockroachColRoachKey:       colblk.DataTypePrefixBytes,
		cockroachColMVCCWallTime:   colblk.DataTypeUint,
		cockroachColMVCCLogical:    colblk.DataTypeUint,
		cockroachColUntypedVersion: colblk.DataTypeBytes,
	},
	NewKeyWriter: func() colblk.KeyWriter {
		kw := &cockroachKeyWriter{}
		kw.roachKeys.Init(16)
		kw.wallTimes.Init()
		kw.logicalTimes.InitWithDefault()
		kw.untypedVersions.Init()
		return kw
	},
	NewKeySeeker: func() colblk.KeySeeker {
		return &cockroachKeySeeker{}
	},
}

type cockroachKeyWriter struct {
	roachKeys       colblk.PrefixBytesBuilder
	wallTimes       colblk.UintBuilder
	logicalTimes    colblk.UintBuilder
	untypedVersions colblk.RawBytesBuilder
	prevSuffix      []byte
}

// Assert *cockroachKeyWriter implements colblk.KeyWriter.
var _ colblk.KeyWriter = (*cockroachKeyWriter)(nil)

func (kw *cockroachKeyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	var cmpv colblk.KeyComparison
	cmpv.PrefixLen = int32(EngineKeySplit(key)) // TODO(jackson): Inline
	if kw.roachKeys.Rows() == 0 {
		cmpv.UserKeyComparison = 1
		return cmpv
	}
	lp := kw.roachKeys.UnsafeGet(kw.roachKeys.Rows() - 1)
	cmpv.CommonPrefixLen = int32(crbytes.CommonPrefix(lp, key[:cmpv.PrefixLen-1]))
	if cmpv.CommonPrefixLen == cmpv.PrefixLen-1 {
		// Adjust CommonPrefixLen to include the sentinel byte.
		cmpv.CommonPrefixLen = cmpv.PrefixLen
		cmpv.UserKeyComparison = int32(EnginePointSuffixCompare(key[cmpv.PrefixLen:], kw.prevSuffix))
		return cmpv
	}
	// The keys have different MVCC prefixes. We haven't determined which is
	// greater, but we know the index at which they diverge. The base.Comparer
	// contract dictates that prefixes must be lexicographically ordered.
	if len(lp) == int(cmpv.CommonPrefixLen) {
		// cmpv.PrefixLen > cmpv.PrefixLenShared; key is greater.
		cmpv.UserKeyComparison = +1
	} else {
		// Both keys have at least 1 additional byte at which they diverge.
		// Compare the diverging byte.
		cmpv.UserKeyComparison = int32(cmp.Compare(key[cmpv.CommonPrefixLen], lp[cmpv.CommonPrefixLen]))
	}
	return cmpv
}

func (kw *cockroachKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	if len(key) == 0 {
		panic(errors.AssertionFailedf("empty key"))
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(key[len(key)-1])
	if (len(key)-versionLen) != int(keyPrefixLen) || key[keyPrefixLen-1] != 0x00 {
		panic(errors.AssertionFailedf("invalid %d-byte key with %d-byte prefix (%q)",
			len(key), keyPrefixLen, key))
	}
	// TODO(jackson): Avoid copying the previous suffix.
	kw.prevSuffix = append(kw.prevSuffix[:0], key[keyPrefixLen:]...)

	// When the roach key is the same, keyPrefixLenSharedWithPrev includes the
	// separator byte.
	kw.roachKeys.Put(key[:keyPrefixLen-1], min(int(keyPrefixLenSharedWithPrev), int(keyPrefixLen)-1))

	// NB: The w.logicalTimes builder was initialized with InitWithDefault, so
	// if we don't set a value, the column value is implicitly zero. We only
	// need to Set anything for non-zero values.
	var wallTime uint64
	var untypedVersion []byte
	switch versionLen {
	case 0:
		// No-op.
	case 9:
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
	case 13, 14:
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
		kw.logicalTimes.Set(row, uint64(binary.BigEndian.Uint32(key[keyPrefixLen+8:keyPrefixLen+12])))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
	default:
		// Not a MVCC timestamp.
		untypedVersion = key[keyPrefixLen:]
	}
	kw.wallTimes.Set(row, wallTime)
	kw.untypedVersions.Put(untypedVersion)
}

func (kw *cockroachKeyWriter) MaterializeKey(dst []byte, i int) []byte {
	dst = append(dst, kw.roachKeys.UnsafeGet(i)...)
	// Append separator byte.
	dst = append(dst, 0)
	if untypedVersion := kw.untypedVersions.UnsafeGet(i); len(untypedVersion) > 0 {
		dst = append(dst, untypedVersion...)
		return dst
	}
	wall := kw.wallTimes.Get(i)
	logical := uint32(kw.logicalTimes.Get(i))
	if logical == 0 {
		if wall == 0 {
			return dst
		}
		dst = append(dst, make([]byte, 9)...)
		binary.BigEndian.PutUint64(dst[len(dst)-9:], wall)
		dst[len(dst)-1] = 9 // Version length byte
		return dst
	}
	dst = append(dst, make([]byte, 13)...)
	binary.BigEndian.PutUint64(dst[len(dst)-13:], wall)
	binary.BigEndian.PutUint32(dst[len(dst)-5:], logical)
	dst[len(dst)-1] = 13 // Version length byte
	return dst
}

func (kw *cockroachKeyWriter) Reset() {
	kw.roachKeys.Reset()
	kw.wallTimes.Reset()
	kw.logicalTimes.Reset()
	kw.untypedVersions.Reset()
}

func (kw *cockroachKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "prefixes: ")
	kw.roachKeys.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "wall times: ")
	kw.wallTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "logical times: ")
	kw.logicalTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "untyped suffixes: ")
	kw.untypedVersions.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (kw *cockroachKeyWriter) NumColumns() int {
	return cockroachColCount
}

func (kw *cockroachKeyWriter) DataType(col int) colblk.DataType {
	return keySchema.ColumnTypes[col]
}

func (kw *cockroachKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = kw.roachKeys.Size(rows, offset)
	offset = kw.wallTimes.Size(rows, offset)
	offset = kw.logicalTimes.Size(rows, offset)
	offset = kw.untypedVersions.Size(rows, offset)
	return offset
}

func (kw *cockroachKeyWriter) Finish(
	col int, rows int, offset uint32, buf []byte,
) (endOffset uint32) {
	switch col {
	case cockroachColRoachKey:
		return kw.roachKeys.Finish(0, rows, offset, buf)
	case cockroachColMVCCWallTime:
		return kw.wallTimes.Finish(0, rows, offset, buf)
	case cockroachColMVCCLogical:
		return kw.logicalTimes.Finish(0, rows, offset, buf)
	case cockroachColUntypedVersion:
		return kw.untypedVersions.Finish(0, rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

var cockroachKeySeekerPool = sync.Pool{
	New: func() interface{} { return &cockroachKeySeeker{} },
}

type cockroachKeySeeker struct {
	roachKeys       colblk.PrefixBytes
	roachKeyChanged colblk.Bitmap
	mvccWallTimes   colblk.UnsafeUints
	mvccLogical     colblk.UnsafeUints
	untypedVersions colblk.RawBytes
}

var _ colblk.KeySeeker = (*cockroachKeySeeker)(nil)

// Init is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Init(d *colblk.DataBlockDecoder) error {
	bd := d.BlockDecoder()
	ks.roachKeys = bd.PrefixBytes(cockroachColRoachKey)
	ks.roachKeyChanged = d.PrefixChanged()
	ks.mvccWallTimes = bd.Uints(cockroachColMVCCWallTime)
	ks.mvccLogical = bd.Uints(cockroachColMVCCLogical)
	ks.untypedVersions = bd.RawBytes(cockroachColUntypedVersion)
	return nil
}

// IsLowerBound compares the provided key to the first user key
// contained within the data block. It's equivalent to performing
//
//	Compare(firstUserKey, k) >= 0
func (ks *cockroachKeySeeker) IsLowerBound(k []byte, syntheticSuffix []byte) bool {
	ek, ok := DecodeEngineKey(k)
	if !ok {
		panic(errors.AssertionFailedf("invalid key %q", k))
	}
	if v := bytes.Compare(ks.roachKeys.UnsafeFirstSlice(), ek.Key); v != 0 {
		return v > 0
	}
	// If there's a synthetic suffix, we ignore the block's suffix columns and
	// compare the key's suffix to the synthetic suffix.
	if len(syntheticSuffix) > 0 {
		return EnginePointSuffixCompare(syntheticSuffix, k[len(ek.Key)+1:]) >= 0
	}
	var wallTime uint64
	var logicalTime uint32
	switch len(ek.Version) {
	case engineKeyNoVersion:
	case engineKeyVersionWallTimeLen:
		wallTime = binary.BigEndian.Uint64(ek.Version[:8])
	case engineKeyVersionWallAndLogicalTimeLen, engineKeyVersionWallLogicalAndSyntheticTimeLen:
		wallTime = binary.BigEndian.Uint64(ek.Version[:8])
		logicalTime = binary.BigEndian.Uint32(ek.Version[8:12])
	default:
		// The provided key `k` is not a MVCC key. Assert that the first key in
		// the block is also not an MVCC key. If it were, that would mean there
		// exists both a MVCC key and a non-MVCC key with the same prefix.
		//
		// TODO(jackson): Double check that we'll never produce index separators
		// that are invalid version lengths.
		if buildutil.CrdbTestBuild && ks.mvccWallTimes.At(0) != 0 {
			panic("comparing timestamp with untyped suffix")
		}
		return EnginePointSuffixCompare(ks.untypedVersions.At(0), ek.Version) >= 0
	}

	// NB: The sign comparison is inverted because suffixes are sorted such that
	// the largest timestamps are "smaller" in the lexicographical ordering.
	if v := cmp.Compare(ks.mvccWallTimes.At(0), wallTime); v != 0 {
		return v < 0
	}
	return cmp.Compare(uint32(ks.mvccLogical.At(0)), logicalTime) <= 0
}

// SeekGE is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) SeekGE(
	key []byte, boundRow int, searchDir int8,
) (row int, equalPrefix bool) {
	// TODO(jackson): Inline EngineKeySplit.
	si := EngineKeySplit(key)
	row, eq := ks.roachKeys.Search(key[:si-1])
	if eq {
		return ks.seekGEOnSuffix(row, key[si:]), true
	}
	return row, false
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ks *cockroachKeySeeker) seekGEOnSuffix(index int, seekSuffix []byte) (row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	const withWall = 9
	const withLogical = withWall + 4
	const withSynthetic = withLogical + 1
	var seekWallTime uint64
	var seekLogicalTime uint32
	switch len(seekSuffix) {
	case 0:
		// The search key has no suffix, so it's the smallest possible key with
		// its prefix. Return the row. This is a common case where the user is
		// seeking to the most-recent row and just wants the smallest key with
		// the prefix.
		return index
	case withLogical, withSynthetic:
		seekWallTime = binary.BigEndian.Uint64(seekSuffix)
		seekLogicalTime = binary.BigEndian.Uint32(seekSuffix[8:])
	case withWall:
		seekWallTime = binary.BigEndian.Uint64(seekSuffix)
	default:
		// The suffix is untyped. Compare the untyped suffixes.
		// Binary search between [index, prefixChanged.SeekSetBitGE(index+1)].
		//
		// Define f(i) = true iff key at i is >= seek key.
		// Invariant: f(l-1) == false, f(u) == true.
		l := index
		u := ks.roachKeyChanged.SeekSetBitGE(index + 1)
		for l < u {
			h := int(uint(l+u) >> 1) // avoid overflow when computing h
			// l ≤ h < u
			if bytes.Compare(ks.untypedVersions.At(h), seekSuffix) >= 0 {
				u = h // preserves f(u) == true
			} else {
				l = h + 1 // preserves f(l-1) == false
			}
		}
		return l
	}
	// Seeking among MVCC versions using a MVCC timestamp.

	// TODO(jackson): What if the row has an untyped suffix?

	// First check the suffix at index, because querying for the latest value is
	// the most common case.
	if latestWallTime := ks.mvccWallTimes.At(index); latestWallTime < seekWallTime ||
		(latestWallTime == seekWallTime && uint32(ks.mvccLogical.At(index)) <= seekLogicalTime) {
		return index
	}

	// Binary search between [index+1, prefixChanged.SeekSetBitGE(index+1)].
	//
	// Define f(i) = true iff key at i is >= seek key.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index + 1
	u := ks.roachKeyChanged.SeekSetBitGE(index + 1)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		hWallTime := ks.mvccWallTimes.At(h)
		if hWallTime < seekWallTime ||
			(hWallTime == seekWallTime && uint32(ks.mvccLogical.At(h)) <= seekLogicalTime) {
			u = h // preserves f(u) = true
		} else {
			l = h + 1 // preserves f(l-1) = false
		}
	}
	return l
}

// MaterializeUserKey is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKey(
	ki *colblk.PrefixBytesIter, prevRow, row int,
) []byte {
	if prevRow+1 == row && prevRow >= 0 {
		ks.roachKeys.SetNext(ki)
	} else {
		ks.roachKeys.SetAt(ki, row)
	}

	roachKeyLen := len(ki.Buf)
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(ki.Buf))) + uintptr(roachKeyLen))
	mvccWall := ks.mvccWallTimes.At(row)
	mvccLogical := uint32(ks.mvccLogical.At(row))
	if mvccWall == 0 && mvccLogical == 0 {
		// This is not an MVCC key. Use the untyped suffix.
		untypedVersion := ks.untypedVersions.At(row)
		if len(untypedVersion) == 0 {
			res := ki.Buf[:roachKeyLen+1]
			res[roachKeyLen] = 0
			return res
		}
		// Slice first, to check that the capacity is sufficient.
		res := ki.Buf[:roachKeyLen+1+len(untypedVersion)]
		*(*byte)(ptr) = 0
		memmove(
			unsafe.Pointer(uintptr(ptr)+1),
			unsafe.Pointer(unsafe.SliceData(untypedVersion)),
			uintptr(len(untypedVersion)),
		)
		return res
	}

	// Inline binary.BigEndian.PutUint64. Note that this code is converted into
	// word-size instructions by the compiler.
	*(*byte)(ptr) = 0
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccWall >> 56)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccWall >> 48)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccWall >> 40)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = byte(mvccWall >> 32)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 5)) = byte(mvccWall >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 6)) = byte(mvccWall >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 7)) = byte(mvccWall >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 8)) = byte(mvccWall)

	ptr = unsafe.Pointer(uintptr(ptr) + 9)
	// This is an MVCC key.
	if mvccLogical == 0 {
		*(*byte)(ptr) = 9
		return ki.Buf[:len(ki.Buf)+10]
	}

	// Inline binary.BigEndian.PutUint32.
	*(*byte)(ptr) = byte(mvccLogical >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccLogical >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccLogical >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccLogical)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = 13
	return ki.Buf[:len(ki.Buf)+14]
}

// MaterializeUserKeyWithSyntheticSuffix is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKeyWithSyntheticSuffix(
	ki *colblk.PrefixBytesIter, suffix []byte, prevRow, row int,
) []byte {
	if prevRow+1 == row && prevRow >= 0 {
		ks.roachKeys.SetNext(ki)
	} else {
		ks.roachKeys.SetAt(ki, row)
	}

	// Slice first, to check that the capacity is sufficient.
	res := ki.Buf[:len(ki.Buf)+1+len(suffix)]
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(ki.Buf))) + uintptr(len(ki.Buf)))
	*(*byte)(ptr) = 0
	memmove(unsafe.Pointer(uintptr(ptr)+1), unsafe.Pointer(unsafe.SliceData(suffix)), uintptr(len(suffix)))
	return res
}

// Release is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Release() {
	*ks = cockroachKeySeeker{}
	cockroachKeySeekerPool.Put(ks)
}

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)
