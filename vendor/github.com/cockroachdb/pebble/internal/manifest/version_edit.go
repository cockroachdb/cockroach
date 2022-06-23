// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(peter): describe the MANIFEST file format, independently of the C++
// project.

var errCorruptManifest = base.CorruptionErrorf("pebble: corrupt manifest")

type byteReader interface {
	io.ByteReader
	io.Reader
}

// Tags for the versionEdit disk format.
// Tag 8 is no longer used.
const (
	// LevelDB tags.
	tagComparator     = 1
	tagLogNumber      = 2
	tagNextFileNumber = 3
	tagLastSequence   = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
	tagPrevLogNumber  = 9

	// RocksDB tags.
	tagNewFile2         = 100
	tagNewFile3         = 102
	tagNewFile4         = 103
	tagColumnFamily     = 200
	tagColumnFamilyAdd  = 201
	tagColumnFamilyDrop = 202
	tagMaxColumnFamily  = 203

	// Pebble tags.
	tagNewFile5 = 104 // Range keys.

	// The custom tags sub-format used by tagNewFile4 and above.
	customTagTerminate         = 1
	customTagNeedsCompaction   = 2
	customTagCreationTime      = 6
	customTagPathID            = 65
	customTagNonSafeIgnoreMask = 1 << 6
)

// DeletedFileEntry holds the state for a file deletion from a level. The file
// itself might still be referenced by another level.
type DeletedFileEntry struct {
	Level   int
	FileNum base.FileNum
}

// NewFileEntry holds the state for a new file or one moved from a different
// level.
type NewFileEntry struct {
	Level int
	Meta  *FileMetadata
}

// VersionEdit holds the state for an edit to a Version along with other
// on-disk state (log numbers, next file number, and the last sequence number).
type VersionEdit struct {
	// ComparerName is the value of Options.Comparer.Name. This is only set in
	// the first VersionEdit in a manifest (either when the DB is created, or
	// when a new manifest is created) and is used to verify that the comparer
	// specified at Open matches the comparer that was previously used.
	ComparerName string

	// MinUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	//
	// This is an optional field, and 0 represents it is not set.
	MinUnflushedLogNum base.FileNum

	// ObsoletePrevLogNum is a historic artifact from LevelDB that is not used by
	// Pebble, RocksDB, or even LevelDB. Its use in LevelDB was deprecated in
	// 6/2011. We keep it around purely for informational purposes when
	// displaying MANIFEST contents.
	ObsoletePrevLogNum uint64

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	NextFileNum base.FileNum

	// LastSeqNum is an upper bound on the sequence numbers that have been
	// assigned in flushed WALs. Unflushed WALs (that will be replayed during
	// recovery) may contain sequence numbers greater than this value.
	LastSeqNum uint64

	// A file num may be present in both deleted files and new files when it
	// is moved from a lower level to a higher level (when the compaction
	// found that there was no overlapping file at the higher level).
	DeletedFiles map[DeletedFileEntry]*FileMetadata
	NewFiles     []NewFileEntry
}

// Decode decodes an edit from the specified reader.
func (v *VersionEdit) Decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	d := versionEditDecoder{br}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch tag {
		case tagComparator:
			s, err := d.readBytes()
			if err != nil {
				return err
			}
			v.ComparerName = string(s)

		case tagLogNumber:
			n, err := d.readFileNum()
			if err != nil {
				return err
			}
			v.MinUnflushedLogNum = n

		case tagNextFileNumber:
			n, err := d.readFileNum()
			if err != nil {
				return err
			}
			v.NextFileNum = n

		case tagLastSequence:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.LastSeqNum = n

		case tagCompactPointer:
			if _, err := d.readLevel(); err != nil {
				return err
			}
			if _, err := d.readBytes(); err != nil {
				return err
			}
			// NB: RocksDB does not use compaction pointers anymore.

		case tagDeletedFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readFileNum()
			if err != nil {
				return err
			}
			if v.DeletedFiles == nil {
				v.DeletedFiles = make(map[DeletedFileEntry]*FileMetadata)
			}
			v.DeletedFiles[DeletedFileEntry{level, fileNum}] = nil

		case tagNewFile, tagNewFile2, tagNewFile3, tagNewFile4, tagNewFile5:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readFileNum()
			if err != nil {
				return err
			}
			if tag == tagNewFile3 {
				// The pathID field appears unused in RocksDB.
				_ /* pathID */, err := d.readUvarint()
				if err != nil {
					return err
				}
			}
			size, err := d.readUvarint()
			if err != nil {
				return err
			}
			// We read the smallest / largest key bounds differently depending on
			// whether we have point, range or both types of keys present in the
			// table.
			var (
				smallestPointKey, largestPointKey []byte
				smallestRangeKey, largestRangeKey []byte
				parsedPointBounds                 bool
				boundsMarker                      byte
			)
			if tag != tagNewFile5 {
				// Range keys not present in the table. Parse the point key bounds.
				smallestPointKey, err = d.readBytes()
				if err != nil {
					return err
				}
				largestPointKey, err = d.readBytes()
				if err != nil {
					return err
				}
			} else {
				// Range keys are present in the table. Determine whether we have point
				// keys to parse, in addition to the bounds.
				boundsMarker, err = d.ReadByte()
				if err != nil {
					return err
				}
				// Parse point key bounds, if present.
				if boundsMarker&maskContainsPointKeys > 0 {
					smallestPointKey, err = d.readBytes()
					if err != nil {
						return err
					}
					largestPointKey, err = d.readBytes()
					if err != nil {
						return err
					}
					parsedPointBounds = true
				} else {
					// The table does not have point keys.
					// Sanity check: the bounds must be range keys.
					if boundsMarker&maskSmallest != 0 || boundsMarker&maskLargest != 0 {
						return base.CorruptionErrorf(
							"new-file-4-range-keys: table without point keys has point key bounds: marker=%x",
							boundsMarker,
						)
					}
				}
				// Parse range key bounds.
				smallestRangeKey, err = d.readBytes()
				if err != nil {
					return err
				}
				largestRangeKey, err = d.readBytes()
				if err != nil {
					return err
				}
			}
			var smallestSeqNum uint64
			var largestSeqNum uint64
			if tag != tagNewFile {
				smallestSeqNum, err = d.readUvarint()
				if err != nil {
					return err
				}
				largestSeqNum, err = d.readUvarint()
				if err != nil {
					return err
				}
			}
			var markedForCompaction bool
			var creationTime uint64
			if tag == tagNewFile4 || tag == tagNewFile5 {
				for {
					customTag, err := d.readUvarint()
					if err != nil {
						return err
					}
					if customTag == customTagTerminate {
						break
					}
					field, err := d.readBytes()
					if err != nil {
						return err
					}
					switch customTag {
					case customTagNeedsCompaction:
						if len(field) != 1 {
							return base.CorruptionErrorf("new-file4: need-compaction field wrong size")
						}
						markedForCompaction = (field[0] == 1)

					case customTagCreationTime:
						var n int
						creationTime, n = binary.Uvarint(field)
						if n != len(field) {
							return base.CorruptionErrorf("new-file4: invalid file creation time")
						}

					case customTagPathID:
						return base.CorruptionErrorf("new-file4: path-id field not supported")

					default:
						if (customTag & customTagNonSafeIgnoreMask) != 0 {
							return base.CorruptionErrorf("new-file4: custom field not supported: %d", customTag)
						}
					}
				}
			}
			m := &FileMetadata{
				FileNum:             fileNum,
				Size:                size,
				CreationTime:        int64(creationTime),
				SmallestSeqNum:      smallestSeqNum,
				LargestSeqNum:       largestSeqNum,
				MarkedForCompaction: markedForCompaction,
			}
			if tag != tagNewFile5 { // no range keys present
				m.SmallestPointKey = base.DecodeInternalKey(smallestPointKey)
				m.LargestPointKey = base.DecodeInternalKey(largestPointKey)
				m.HasPointKeys = true
				m.Smallest, m.Largest = m.SmallestPointKey, m.LargestPointKey
				m.boundTypeSmallest, m.boundTypeLargest = boundTypePointKey, boundTypePointKey
			} else { // range keys present
				// Set point key bounds, if parsed.
				if parsedPointBounds {
					m.SmallestPointKey = base.DecodeInternalKey(smallestPointKey)
					m.LargestPointKey = base.DecodeInternalKey(largestPointKey)
					m.HasPointKeys = true
				}
				// Set range key bounds.
				m.SmallestRangeKey = base.DecodeInternalKey(smallestRangeKey)
				m.LargestRangeKey = base.DecodeInternalKey(largestRangeKey)
				m.HasRangeKeys = true
				// Set overall bounds (by default assume range keys).
				m.Smallest, m.Largest = m.SmallestRangeKey, m.LargestRangeKey
				m.boundTypeSmallest, m.boundTypeLargest = boundTypeRangeKey, boundTypeRangeKey
				if boundsMarker&maskSmallest == maskSmallest {
					m.Smallest = m.SmallestPointKey
					m.boundTypeSmallest = boundTypePointKey
				}
				if boundsMarker&maskLargest == maskLargest {
					m.Largest = m.LargestPointKey
					m.boundTypeLargest = boundTypePointKey
				}
			}
			m.boundsSet = true
			v.NewFiles = append(v.NewFiles, NewFileEntry{
				Level: level,
				Meta:  m,
			})

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.ObsoletePrevLogNum = n

		case tagColumnFamily, tagColumnFamilyAdd, tagColumnFamilyDrop, tagMaxColumnFamily:
			return base.CorruptionErrorf("column families are not supported")

		default:
			return errCorruptManifest
		}
	}
	return nil
}

// Encode encodes an edit to the specified writer.
func (v *VersionEdit) Encode(w io.Writer) error {
	e := versionEditEncoder{new(bytes.Buffer)}

	if v.ComparerName != "" {
		e.writeUvarint(tagComparator)
		e.writeString(v.ComparerName)
	}
	if v.MinUnflushedLogNum != 0 {
		e.writeUvarint(tagLogNumber)
		e.writeUvarint(uint64(v.MinUnflushedLogNum))
	}
	if v.ObsoletePrevLogNum != 0 {
		e.writeUvarint(tagPrevLogNumber)
		e.writeUvarint(v.ObsoletePrevLogNum)
	}
	if v.NextFileNum != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(uint64(v.NextFileNum))
	}
	// RocksDB requires LastSeqNum to be encoded for the first MANIFEST entry,
	// even though its value is zero. We detect this by encoding LastSeqNum when
	// ComparerName is set.
	if v.LastSeqNum != 0 || v.ComparerName != "" {
		e.writeUvarint(tagLastSequence)
		e.writeUvarint(v.LastSeqNum)
	}
	for x := range v.DeletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(uint64(x.FileNum))
	}
	for _, x := range v.NewFiles {
		customFields := x.Meta.MarkedForCompaction || x.Meta.CreationTime != 0
		var tag uint64
		switch {
		case x.Meta.HasRangeKeys:
			tag = tagNewFile5
		case customFields:
			tag = tagNewFile4
		default:
			tag = tagNewFile2
		}
		e.writeUvarint(tag)
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(uint64(x.Meta.FileNum))
		e.writeUvarint(x.Meta.Size)
		if !x.Meta.HasRangeKeys {
			// If we have no range keys, preserve the original format and write the
			// smallest and largest point keys.
			e.writeKey(x.Meta.SmallestPointKey)
			e.writeKey(x.Meta.LargestPointKey)
		} else {
			// When range keys are present, we first write a marker byte that
			// indicates if the table also contains point keys, in addition to how the
			// overall bounds for the table should be reconstructed. This byte is
			// followed by the keys themselves.
			b, err := x.Meta.boundsMarker()
			if err != nil {
				return err
			}
			if err = e.WriteByte(b); err != nil {
				return err
			}
			// Write point key bounds (if present).
			if x.Meta.HasPointKeys {
				e.writeKey(x.Meta.SmallestPointKey)
				e.writeKey(x.Meta.LargestPointKey)
			}
			// Write range key bounds.
			e.writeKey(x.Meta.SmallestRangeKey)
			e.writeKey(x.Meta.LargestRangeKey)
		}
		e.writeUvarint(x.Meta.SmallestSeqNum)
		e.writeUvarint(x.Meta.LargestSeqNum)
		if customFields {
			if x.Meta.CreationTime != 0 {
				e.writeUvarint(customTagCreationTime)
				var buf [binary.MaxVarintLen64]byte
				n := binary.PutUvarint(buf[:], uint64(x.Meta.CreationTime))
				e.writeBytes(buf[:n])
			}
			if x.Meta.MarkedForCompaction {
				e.writeUvarint(customTagNeedsCompaction)
				e.writeBytes([]byte{1})
			}
			e.writeUvarint(customTagTerminate)
		}
	}
	_, err := w.Write(e.Bytes())
	return err
}

type versionEditDecoder struct {
	byteReader
}

func (d versionEditDecoder) readBytes() ([]byte, error) {
	n, err := d.readUvarint()
	if err != nil {
		return nil, err
	}
	s := make([]byte, n)
	_, err = io.ReadFull(d, s)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, errCorruptManifest
		}
		return nil, err
	}
	return s, nil
}

func (d versionEditDecoder) readLevel() (int, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	if u >= NumLevels {
		return 0, errCorruptManifest
	}
	return int(u), nil
}

func (d versionEditDecoder) readFileNum() (base.FileNum, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	return base.FileNum(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptManifest
		}
		return 0, err
	}
	return u, nil
}

type versionEditEncoder struct {
	*bytes.Buffer
}

func (e versionEditEncoder) writeBytes(p []byte) {
	e.writeUvarint(uint64(len(p)))
	e.Write(p)
}

func (e versionEditEncoder) writeKey(k InternalKey) {
	e.writeUvarint(uint64(k.Size()))
	e.Write(k.UserKey)
	buf := k.EncodeTrailer()
	e.Write(buf[:])
}

func (e versionEditEncoder) writeString(s string) {
	e.writeUvarint(uint64(len(s)))
	e.WriteString(s)
}

func (e versionEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

// BulkVersionEdit summarizes the files added and deleted from a set of version
// edits.
type BulkVersionEdit struct {
	Added   [NumLevels][]*FileMetadata
	Deleted [NumLevels]map[base.FileNum]*FileMetadata

	// AddedByFileNum maps file number to file metadata for all added files
	// from accumulated version edits. AddedByFileNum is only populated if set
	// to non-nil by a caller. It must be set to non-nil when replaying
	// version edits read from a MANIFEST (as opposed to VersionEdits
	// constructed in-memory).  While replaying a MANIFEST file,
	// VersionEdit.DeletedFiles map entries have nil values, because the
	// on-disk deletion record encodes only the file number. Accumulate
	// uses AddedByFileNum to correctly populate the BulkVersionEdit's Deleted
	// field with non-nil *FileMetadata.
	AddedByFileNum map[base.FileNum]*FileMetadata

	// MarkedForCompactionCountDiff holds the aggregated count of files
	// marked for compaction added or removed.
	MarkedForCompactionCountDiff int
}

// Accumulate adds the file addition and deletions in the specified version
// edit to the bulk edit's internal state.
func (b *BulkVersionEdit) Accumulate(ve *VersionEdit) error {
	for df, m := range ve.DeletedFiles {
		dmap := b.Deleted[df.Level]
		if dmap == nil {
			dmap = make(map[base.FileNum]*FileMetadata)
			b.Deleted[df.Level] = dmap
		}

		if m == nil {
			// m is nil only when replaying a MANIFEST.
			if b.AddedByFileNum == nil {
				return errors.Errorf("deleted file L%d.%s's metadata is absent and bve.AddedByFileNum is nil", df.Level, df.FileNum)
			}
			m = b.AddedByFileNum[df.FileNum]
			if m == nil {
				return base.CorruptionErrorf("pebble: file deleted L%d.%s before it was inserted", df.Level, df.FileNum)
			}
		}
		if m.MarkedForCompaction {
			b.MarkedForCompactionCountDiff--
		}
		dmap[df.FileNum] = m
	}

	for _, nf := range ve.NewFiles {
		// A new file should not have been deleted in this or a preceding
		// VersionEdit at the same level (though files can move across levels).
		if dmap := b.Deleted[nf.Level]; dmap != nil {
			if _, ok := dmap[nf.Meta.FileNum]; ok {
				return base.CorruptionErrorf("pebble: file deleted L%d.%s before it was inserted", nf.Level, nf.Meta.FileNum)
			}
		}
		b.Added[nf.Level] = append(b.Added[nf.Level], nf.Meta)
		if b.AddedByFileNum != nil {
			b.AddedByFileNum[nf.Meta.FileNum] = nf.Meta
		}
		if nf.Meta.MarkedForCompaction {
			b.MarkedForCompactionCountDiff++
		}
	}
	return nil
}

// Apply applies the delta b to the current version to produce a new
// version. The new version is consistent with respect to the comparer cmp.
//
// curr may be nil, which is equivalent to a pointer to a zero version.
//
// On success, a map of zombie files containing the file numbers and sizes of
// deleted files is returned. These files are considered zombies because they
// are no longer referenced by the returned Version, but cannot be deleted from
// disk as they are still in use by the incoming Version.
func (b *BulkVersionEdit) Apply(
	curr *Version,
	cmp Compare,
	formatKey base.FormatKey,
	flushSplitBytes int64,
	readCompactionRate int64,
) (_ *Version, zombies map[base.FileNum]uint64, _ error) {
	addZombie := func(fileNum base.FileNum, size uint64) {
		if zombies == nil {
			zombies = make(map[base.FileNum]uint64)
		}
		zombies[fileNum] = size
	}
	// The remove zombie function is used to handle tables that are moved from
	// one level to another during a version edit (i.e. a "move" compaction).
	removeZombie := func(fileNum base.FileNum) {
		if zombies != nil {
			delete(zombies, fileNum)
		}
	}

	v := new(Version)

	// Adjust the count of files marked for compaction.
	if curr != nil {
		v.Stats.MarkedForCompaction = curr.Stats.MarkedForCompaction
	}
	v.Stats.MarkedForCompaction += b.MarkedForCompactionCountDiff
	if v.Stats.MarkedForCompaction < 0 {
		return nil, nil, base.CorruptionErrorf("pebble: version marked for compaction count negative")
	}

	for level := range v.Levels {
		if curr == nil || curr.Levels[level].tree.root == nil {
			v.Levels[level] = makeLevelMetadata(cmp, level, nil /* files */)
		} else {
			v.Levels[level] = curr.Levels[level].clone()
		}
		if curr == nil || curr.RangeKeyLevels[level].tree.root == nil {
			v.RangeKeyLevels[level] = makeLevelMetadata(cmp, level, nil /* files */)
		} else {
			v.RangeKeyLevels[level] = curr.RangeKeyLevels[level].clone()
		}

		if len(b.Added[level]) == 0 && len(b.Deleted[level]) == 0 {
			// There are no edits on this level.
			if level == 0 {
				// Initialize L0Sublevels.
				if curr == nil || curr.L0Sublevels == nil {
					if err := v.InitL0Sublevels(cmp, formatKey, flushSplitBytes); err != nil {
						return nil, nil, errors.Wrap(err, "pebble: internal error")
					}
				} else {
					v.L0Sublevels = curr.L0Sublevels
					v.L0SublevelFiles = v.L0Sublevels.Levels
				}
			}
			continue
		}

		// Some edits on this level.
		lm := &v.Levels[level]
		lmRange := &v.RangeKeyLevels[level]
		addedFiles := b.Added[level]
		deletedMap := b.Deleted[level]
		if n := v.Levels[level].Len() + len(addedFiles); n == 0 {
			return nil, nil, base.CorruptionErrorf(
				"pebble: internal error: No current or added files but have deleted files: %d",
				errors.Safe(len(deletedMap)))
		}

		// NB: addedFiles may be empty and it also is not necessarily
		// internally consistent: it does not reflect deletions in deletedMap.

		for _, f := range deletedMap {
			addZombie(f.FileNum, f.Size)
			if obsolete := v.Levels[level].tree.delete(f); obsolete {
				// Deleting a file from the B-Tree may decrement its
				// reference count. However, because we cloned the
				// previous level's B-Tree, this should never result in a
				// file's reference count dropping to zero.
				err := errors.Errorf("pebble: internal error: file L%d.%s obsolete during B-Tree removal", level, f.FileNum)
				return nil, nil, err
			}
			if f.HasRangeKeys {
				if obsolete := v.RangeKeyLevels[level].tree.delete(f); obsolete {
					// Deleting a file from the B-Tree may decrement its
					// reference count. However, because we cloned the
					// previous level's B-Tree, this should never result in a
					// file's reference count dropping to zero.
					err := errors.Errorf("pebble: internal error: file L%d.%s obsolete during range-key B-Tree removal", level, f.FileNum)
					return nil, nil, err
				}
			}
		}

		var sm, la *FileMetadata
		for _, f := range addedFiles {
			if _, ok := deletedMap[f.FileNum]; ok {
				// Already called addZombie on this file in the preceding
				// loop, so we don't need to do it here.
				continue
			}

			// NB: allowedSeeks is used for read triggered compactions. It is set using
			// Options.Experimental.ReadCompactionRate which defaults to 32KB.
			var allowedSeeks int64
			if readCompactionRate != 0 {
				allowedSeeks = int64(f.Size) / readCompactionRate
			}
			if allowedSeeks < 100 {
				allowedSeeks = 100
			}
			atomic.StoreInt64(&f.Atomic.AllowedSeeks, allowedSeeks)
			f.InitAllowedSeeks = allowedSeeks

			err := lm.tree.insert(f)
			if err != nil {
				return nil, nil, errors.Wrap(err, "pebble")
			}
			if f.HasRangeKeys {
				err = lmRange.tree.insert(f)
				if err != nil {
					return nil, nil, errors.Wrap(err, "pebble")
				}
			}
			removeZombie(f.FileNum)
			// Track the keys with the smallest and largest keys, so that we can
			// check consistency of the modified span.
			if sm == nil || base.InternalCompare(cmp, sm.Smallest, f.Smallest) > 0 {
				sm = f
			}
			if la == nil || base.InternalCompare(cmp, la.Largest, f.Largest) < 0 {
				la = f
			}
		}

		if level == 0 {
			if curr != nil && curr.L0Sublevels != nil && len(deletedMap) == 0 {
				// Flushes and ingestions that do not delete any L0 files do not require
				// a regeneration of L0Sublevels from scratch. We can instead generate
				// it incrementally.
				var err error
				// AddL0Files requires addedFiles to be sorted in seqnum order.
				addedFiles = append([]*FileMetadata(nil), addedFiles...)
				SortBySeqNum(addedFiles)
				v.L0Sublevels, err = curr.L0Sublevels.AddL0Files(addedFiles, flushSplitBytes, &v.Levels[0])
				if errors.Is(err, errInvalidL0SublevelsOpt) {
					err = v.InitL0Sublevels(cmp, formatKey, flushSplitBytes)
				}
				if err != nil {
					return nil, nil, errors.Wrap(err, "pebble: internal error")
				}
				v.L0SublevelFiles = v.L0Sublevels.Levels
			} else if err := v.InitL0Sublevels(cmp, formatKey, flushSplitBytes); err != nil {
				return nil, nil, errors.Wrap(err, "pebble: internal error")
			}
			if err := CheckOrdering(cmp, formatKey, Level(0), v.Levels[level].Iter()); err != nil {
				return nil, nil, errors.Wrap(err, "pebble: internal error")
			}
			continue
		}

		// Check consistency of the level in the vicinity of our edits.
		if sm != nil && la != nil {
			overlap := overlaps(v.Levels[level].Iter(), cmp, sm.Smallest.UserKey,
				la.Largest.UserKey, la.Largest.IsExclusiveSentinel())
			// overlap contains all of the added files. We want to ensure that
			// the added files are consistent with neighboring existing files
			// too, so reslice the overlap to pull in a neighbor on each side.
			check := overlap.Reslice(func(start, end *LevelIterator) {
				if m := start.Prev(); m == nil {
					start.Next()
				}
				if m := end.Next(); m == nil {
					end.Prev()
				}
			})
			if err := CheckOrdering(cmp, formatKey, Level(level), check.Iter()); err != nil {
				return nil, nil, errors.Wrap(err, "pebble: internal error")
			}
		}
	}
	return v, zombies, nil
}
