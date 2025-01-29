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
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
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

// KeySchemas holds the set of KeySchemas understandable by CockroachDB.
var KeySchemas = []*pebble.KeySchema{&keySchema}

// TODO(jackson): We need to rethink uses of DefaultKeySchema when we introduce
// a new key schema.

// DefaultKeySchema is the name of the default key schema.
var DefaultKeySchema = keySchema.Name

var keySchema = colblk.KeySchema{
	Name:       "crdb1",
	HeaderSize: 1,
	ColumnTypes: []colblk.DataType{
		cockroachColRoachKey:       colblk.DataTypePrefixBytes,
		cockroachColMVCCWallTime:   colblk.DataTypeUint,
		cockroachColMVCCLogical:    colblk.DataTypeUint,
		cockroachColUntypedVersion: colblk.DataTypeBytes,
	},
	NewKeyWriter: func() colblk.KeyWriter {
		return makeCockroachKeyWriter()
	},
	InitKeySeekerMetadata: func(meta *colblk.KeySeekerMetadata, d *colblk.DataBlockDecoder) {
		ks := (*cockroachKeySeeker)(unsafe.Pointer(meta))
		ks.init(d)
	},
	KeySeeker: func(meta *colblk.KeySeekerMetadata) colblk.KeySeeker {
		return (*cockroachKeySeeker)(unsafe.Pointer(meta))
	},
}

// suffixTypes is a bitfield indicating what kind of suffixes are present in a
// block.
type suffixTypes uint8

const (
	// hasMVCCSuffixes is set if there is at least one key with an MVCC suffix in
	// the block.
	hasMVCCSuffixes suffixTypes = (1 << iota)
	// hasEmptySuffixes is set if there is at least one key with no suffix in the block.
	hasEmptySuffixes
	// hasNonMVCCSuffixes is set if there is at least one key with a non-empty,
	// non-MVCC suffix.
	hasNonMVCCSuffixes
)

func (s suffixTypes) String() string {
	var suffixes []string
	if s&hasMVCCSuffixes != 0 {
		suffixes = append(suffixes, "mvcc")
	}
	if s&hasEmptySuffixes != 0 {
		suffixes = append(suffixes, "empty")
	}
	if s&hasNonMVCCSuffixes != 0 {
		suffixes = append(suffixes, "non-mvcc")
	}
	if len(suffixes) == 0 {
		return "none"
	}
	return strings.Join(suffixes, ",")
}

type cockroachKeyWriter struct {
	roachKeys       colblk.PrefixBytesBuilder
	wallTimes       colblk.UintBuilder
	logicalTimes    colblk.UintBuilder
	untypedVersions colblk.RawBytesBuilder
	suffixTypes     suffixTypes
	prevRoachKeyLen int32
	prevSuffix      []byte
}

// Assert *cockroachKeyWriter implements colblk.KeyWriter.
var _ colblk.KeyWriter = (*cockroachKeyWriter)(nil)

func makeCockroachKeyWriter() *cockroachKeyWriter {
	kw := &cockroachKeyWriter{}
	kw.roachKeys.Init(16)
	kw.wallTimes.Init()
	kw.logicalTimes.InitWithDefault()
	kw.untypedVersions.Init()
	return kw
}

func (kw *cockroachKeyWriter) Reset() {
	kw.roachKeys.Reset()
	kw.wallTimes.Reset()
	kw.logicalTimes.Reset()
	kw.untypedVersions.Reset()
	kw.suffixTypes = 0
	kw.prevRoachKeyLen = 0
}

func (kw *cockroachKeyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	prefixLen := EngineKeySplit(key)
	if kw.roachKeys.Rows() == 0 {
		return colblk.KeyComparison{
			PrefixLen:         int32(prefixLen),
			CommonPrefixLen:   0,
			UserKeyComparison: +1,
		}
	}
	lastRoachKey := kw.roachKeys.UnsafeGet(kw.roachKeys.Rows() - 1)
	commonPrefixLen := crbytes.CommonPrefix(lastRoachKey, key[:prefixLen-1])
	if len(lastRoachKey) == commonPrefixLen {
		if buildutil.CrdbTestBuild && len(lastRoachKey) > prefixLen-1 {
			panic(errors.AssertionFailedf("out-of-order keys: previous roach key %q > roach key of key %q",
				lastRoachKey, key))
		}
		// All the bytes of the previous roach key form a byte-wise prefix of
		// [key]'s prefix. The last byte of the previous prefix is the 0x00
		// sentinel byte, which is not stored within roachKeys. It's possible
		// that [key] also has a 0x00 byte in the same position (either also
		// serving as a sentinel byte, in which case the prefixes are equal, or
		// not in which case [key] is greater). In both cases, we need to
		// increment CommonPrefixLen.
		if key[commonPrefixLen] == 0x00 {
			commonPrefixLen++
			if commonPrefixLen == prefixLen {
				// The prefixes are equal; compare the suffixes.
				return colblk.KeyComparison{
					PrefixLen:         int32(prefixLen),
					CommonPrefixLen:   int32(commonPrefixLen),
					UserKeyComparison: int32(EnginePointSuffixCompare(key[prefixLen:], kw.prevSuffix)),
				}
			}
		}
		// prefixLen > commonPrefixLen; key is greater.
		return colblk.KeyComparison{
			PrefixLen:         int32(prefixLen),
			CommonPrefixLen:   int32(commonPrefixLen),
			UserKeyComparison: +1,
		}
	}
	// Both keys have at least 1 additional byte at which they diverge.
	// Compare the diverging byte.
	return colblk.KeyComparison{
		PrefixLen:         int32(prefixLen),
		CommonPrefixLen:   int32(commonPrefixLen),
		UserKeyComparison: int32(cmp.Compare(key[commonPrefixLen], lastRoachKey[commonPrefixLen])),
	}
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

	// When the roach key is the same or contain the previous key as a prefix,
	// keyPrefixLenSharedWithPrev includes the previous key's separator byte.
	kw.roachKeys.Put(key[:keyPrefixLen-1], min(int(keyPrefixLenSharedWithPrev), int(kw.prevRoachKeyLen)))
	kw.prevRoachKeyLen = keyPrefixLen - 1

	// NB: The w.logicalTimes builder was initialized with InitWithDefault, so
	// if we don't set a value, the column value is implicitly zero. We only
	// need to Set anything for non-zero values.
	var wallTime uint64
	var untypedVersion []byte
	switch versionLen {
	case 0:
		// No-op.
		kw.suffixTypes |= hasEmptySuffixes
	case 9:
		kw.suffixTypes |= hasMVCCSuffixes
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
	case 13, 14:
		kw.suffixTypes |= hasMVCCSuffixes
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
		kw.logicalTimes.Set(row, uint64(binary.BigEndian.Uint32(key[keyPrefixLen+8:keyPrefixLen+12])))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
	default:
		// Not a MVCC timestamp.
		kw.suffixTypes |= hasNonMVCCSuffixes
		untypedVersion = key[keyPrefixLen : len(key)-1]
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
		dst = append(dst, byte(len(untypedVersion)+1))
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
	fmt.Fprint(dst, "suffix types: ")
	fmt.Fprintln(dst, kw.suffixTypes.String())
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

func (kw *cockroachKeyWriter) FinishHeader(buf []byte) {
	buf[0] = byte(kw.suffixTypes)
}

type cockroachKeySeeker struct {
	roachKeys       colblk.PrefixBytes
	roachKeyChanged colblk.Bitmap
	mvccWallTimes   colblk.UnsafeUints
	mvccLogical     colblk.UnsafeUints
	untypedVersions colblk.RawBytes
	suffixTypes     suffixTypes
}

// Assert that the cockroachKeySeeker fits inside KeySeekerMetadata.
var _ uint = colblk.KeySeekerMetadataSize - uint(unsafe.Sizeof(cockroachKeySeeker{}))

var _ colblk.KeySeeker = (*cockroachKeySeeker)(nil)

func (ks *cockroachKeySeeker) init(d *colblk.DataBlockDecoder) {
	bd := d.BlockDecoder()
	ks.roachKeys = bd.PrefixBytes(cockroachColRoachKey)
	ks.roachKeyChanged = d.PrefixChanged()
	ks.mvccWallTimes = bd.Uints(cockroachColMVCCWallTime)
	ks.mvccLogical = bd.Uints(cockroachColMVCCLogical)
	ks.untypedVersions = bd.RawBytes(cockroachColUntypedVersion)
	header := d.KeySchemaHeader()
	if len(header) != 1 {
		panic(errors.AssertionFailedf("invalid key schema-specific header %x", header))
	}
	ks.suffixTypes = suffixTypes(header[0])
}

// IsLowerBound is part of the KeySeeker interface.
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
	firstRowWall := ks.mvccWallTimes.At(0)
	firstLogical := ks.mvccLogical.At(0)
	// If the first row's WallTime and LogicalTime are both zero, the first row
	// is either (a) unversioned (and sorts before all other suffixes) or (b) is
	// a non-MVCC key with an untyped version.
	if firstRowWall == 0 && firstLogical == 0 {
		return EnginePointSuffixCompare(ks.untypedVersions.At(0), ek.Version) >= 0
	}
	var wallTime uint64
	var logicalTime uint32
	switch len(ek.Version) {
	case engineKeyNoVersion:
		// The zero-length version is the smallest possible suffix.
		return true
	case engineKeyVersionWallTimeLen:
		wallTime = binary.BigEndian.Uint64(ek.Version[:8])
	case engineKeyVersionWallAndLogicalTimeLen, engineKeyVersionWallLogicalAndSyntheticTimeLen:
		wallTime = binary.BigEndian.Uint64(ek.Version[:8])
		logicalTime = binary.BigEndian.Uint32(ek.Version[8:12])
	default:
		// The provided key `k` is not a MVCC key.
		//
		// The first row IS an MVCC key. We don't expect this to happen in
		// practice, so this path is not performance sensitive. We reconsistute
		// the first row's MVCC suffix and then compare it to the provided key's
		// non-MVCC suffix.
		//gcassert:noescape
		var buf [13]byte
		//gcassert:inline
		binary.BigEndian.PutUint64(buf[:], firstRowWall)
		var firstRowSuffix []byte
		if firstLogical == 0 {
			buf[8] = 9
			firstRowSuffix = buf[:9]
		} else {
			//gcassert:inline
			binary.BigEndian.PutUint32(buf[8:], uint32(firstLogical))
			buf[12] = 13
			firstRowSuffix = buf[:13]
		}
		return EnginePointSuffixCompare(firstRowSuffix, ek.Version) >= 0
	}

	// NB: The sign comparison is inverted because suffixes are sorted such that
	// the largest timestamps are "smaller" in the lexicographical ordering.
	if v := cmp.Compare(firstRowWall, wallTime); v != 0 {
		// The empty-suffixed zero-timestamped key sorts first. If the first row
		// has zero wall and logical times, it sorts before k and k is not a
		// lower bound.
		return v < 0
	}
	return cmp.Compare(uint32(firstLogical), logicalTime) <= 0
}

// SeekGE is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) SeekGE(
	key []byte, boundRow int, searchDir int8,
) (row int, equalPrefix bool) {
	if buildutil.CrdbTestBuild && len(key) == 0 {
		panic(errors.AssertionFailedf("seeking to empty key"))
	}
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
	// We have three common cases:
	// 1. The seek key has no suffix.
	// 2. We are seeking to an MVCC timestamp in a block where all keys have
	//    MVCC timestamps (e.g. SQL table data).
	// 3. We are seeking to a non-MVCC timestamp in a block where no keys have
	//    MVCC timestamps (e.g. lock keys).

	if len(seekSuffix) == 0 {
		// The search key has no suffix, so it's the smallest possible key with its
		// prefix. Return the row. This is a common case where the user is seeking
		// to the most-recent row and just wants the smallest key with the prefix.
		return index
	}

	const withWall = 9
	const withLogical = withWall + 4
	const withSynthetic = withLogical + 1

	// If suffixTypes == hasMVCCSuffixes, all keys in the block have MVCC
	// suffixes. Note that blocks that contain both MVCC and non-MVCC should be
	// very rare, so it's ok to use the more general path below in that case.
	if ks.suffixTypes == hasMVCCSuffixes && (len(seekSuffix) == withWall || len(seekSuffix) == withLogical || len(seekSuffix) == withSynthetic) {
		// Fast path: seeking among MVCC versions using a MVCC timestamp.
		seekWallTime := binary.BigEndian.Uint64(seekSuffix)
		var seekLogicalTime uint32
		if len(seekSuffix) >= withLogical {
			seekLogicalTime = binary.BigEndian.Uint32(seekSuffix[8:])
		}

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
			m := int(uint(l+u) >> 1) // avoid overflow when computing m
			// l ≤ m < u
			mWallTime := ks.mvccWallTimes.At(m)
			if mWallTime < seekWallTime ||
				(mWallTime == seekWallTime && uint32(ks.mvccLogical.At(m)) <= seekLogicalTime) {
				u = m // preserves f(u) = true
			} else {
				l = m + 1 // preserves f(l-1) = false
			}
		}
		return l
	}

	// Remove the terminator byte, which we know is equal to len(seekSuffix)
	// because we obtained the suffix by splitting the seek key.
	version := seekSuffix[:len(seekSuffix)-1]
	if buildutil.CrdbTestBuild && seekSuffix[len(version)] != byte(len(seekSuffix)) {
		panic(errors.AssertionFailedf("invalid seek suffix %x", seekSuffix))
	}

	// Binary search for version between [index, prefixChanged.SeekSetBitGE(index+1)].
	//
	// Define f(i) = true iff key at i is >= seek key (i.e. suffix at i is <= seek suffix).
	// Invariant: f(l-1) == false, f(u) == true.
	l := index
	u := ks.roachKeyChanged.SeekSetBitGE(index + 1)

	for l < u {
		m := int(uint(l+u) >> 1) // avoid overflow when computing m
		// l ≤ m < u
		mVer := ks.untypedVersions.At(m)
		if len(mVer) == 0 {
			wallTime := ks.mvccWallTimes.At(m)
			logicalTime := uint32(ks.mvccLogical.At(m))
			if wallTime == 0 && logicalTime == 0 {
				// This row has an empty suffix. Since the seek suffix is not empty, it comes after.
				l = m + 1
				continue
			}

			// Note: this path is not very performance sensitive: blocks that mix MVCC
			// suffixes with non-MVCC suffixes should be rare.

			//gcassert:noescape
			var buf [12]byte
			//gcassert:inline
			binary.BigEndian.PutUint64(buf[:], wallTime)
			if logicalTime == 0 {
				mVer = buf[:8]
			} else {
				//gcassert:inline
				binary.BigEndian.PutUint32(buf[8:], logicalTime)
				mVer = buf[:12]
			}
		}
		if bytes.Compare(mVer, version) <= 0 {
			u = m // preserves f(u) == true
		} else {
			l = m + 1 // preserves f(l-1) == false
		}
	}
	return l
}

// MaterializeUserKey is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKey(
	ki *colblk.PrefixBytesIter, prevRow, row int,
) []byte {
	if buildutil.CrdbTestBuild && (row < 0 || row >= ks.roachKeys.Rows()) {
		panic(errors.AssertionFailedf("invalid row number %d", row))
	}
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
		res := ki.Buf[:roachKeyLen+2+len(untypedVersion)]
		*(*byte)(ptr) = 0
		memmove(
			unsafe.Pointer(uintptr(ptr)+1),
			unsafe.Pointer(unsafe.SliceData(untypedVersion)),
			uintptr(len(untypedVersion)),
		)
		*(*byte)(unsafe.Pointer(uintptr(ptr) + uintptr(len(untypedVersion)+1))) = byte(len(untypedVersion) + 1)
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

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)
