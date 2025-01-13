// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// BackupMetadataDescriptorsListPath is the name of the SST file containing
	// the BackupManifest_Descriptors or BackupManifest_DescriptorRevisions of the
	// backup. This file is always written in conjunction with the
	// `BACKUP_METADATA`.
	BackupMetadataDescriptorsListPath = "descriptorslist.sst"
	sstDescsPrefix                    = "desc/"
)

// WriteDescsSST is responsible for writing the SST containing the Descriptor
// and DescriptorChanges field of the input BackupManifest. If DescriptorChanges
// is non-empty, then the descriptor changes will be written to the SST with the
// MVCC timestamp equal to the revision time. Otherwise, contents of the
// Descriptors field will be written to the SST with an empty MVCC timestamp.
func WriteDescsSST(
	ctx context.Context,
	m *backuppb.BackupManifest,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	path string,
) error {
	w, err := makeWriter(ctx, dest, path, enc, kmsEnv)
	if err != nil {
		return err
	}
	defer w.Close()
	descSST := storage.MakeTransportSSTWriter(ctx, dest.Settings(), w)
	defer descSST.Close()

	if err := writeDescsToMetadata(ctx, descSST, m); err != nil {
		return err
	}

	if err := descSST.Finish(); err != nil {
		return err
	}

	return w.Close()
}

func DescChangesLess(
	left *backuppb.BackupManifest_DescriptorRevision,
	right *backuppb.BackupManifest_DescriptorRevision,
) bool {
	if left.ID != right.ID {
		return left.ID < right.ID
	}

	return !left.Time.Less(right.Time)
}

func writeDescsToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	// Add descriptors from revisions if available, Descriptors if not.
	if len(m.DescriptorChanges) > 0 {
		sort.Slice(m.DescriptorChanges, func(i, j int) bool {
			return DescChangesLess(&m.DescriptorChanges[i], &m.DescriptorChanges[j])
		})
		for _, i := range m.DescriptorChanges {
			k := encodeDescSSTKey(i.ID)
			var b []byte
			if i.Desc != nil {
				t, _, _, _, _ := descpb.GetDescriptors(i.Desc)
				if t == nil || !t.Dropped() {
					bytes, err := protoutil.Marshal(i.Desc)
					if err != nil {
						return err
					}
					b = bytes
				}
			}
			if err := sst.PutRawMVCC(storage.MVCCKey{Key: k, Timestamp: i.Time}, b); err != nil {
				return err
			}

		}
	} else {
		sort.Slice(m.Descriptors, func(i, j int) bool {
			return descID(m.Descriptors[i]) < descID(m.Descriptors[j])
		})
		for _, i := range m.Descriptors {
			id := descID(i)
			k := encodeDescSSTKey(id)
			b, err := protoutil.Marshal(&i)
			if err != nil {
				return err
			}

			// Put descriptors at start time. For non-rev backups this timestamp
			// doesn't matter. For the special case where there were no descriptor
			// changes in an incremental backup, it's helpful to have existing
			// descriptors at the start time, so we don't have to look back further
			// than the very last backup.
			if m.StartTime.IsEmpty() || m.MVCCFilter == backuppb.MVCCFilter_Latest {
				if err := sst.PutUnversioned(k, b); err != nil {
					return err
				}
			} else {
				if err := sst.PutRawMVCC(storage.MVCCKey{Key: k, Timestamp: m.StartTime}, b); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func descID(in descpb.Descriptor) descpb.ID {
	switch i := in.Union.(type) {
	case *descpb.Descriptor_Table:
		return i.Table.ID
	case *descpb.Descriptor_Database:
		return i.Database.ID
	case *descpb.Descriptor_Type:
		return i.Type.ID
	case *descpb.Descriptor_Schema:
		return i.Schema.ID
	case *descpb.Descriptor_Function:
		return i.Function.ID
	default:
		panic(fmt.Sprintf("unknown desc %T", in))
	}
}

func encodeDescSSTKey(id descpb.ID) roachpb.Key {
	return roachpb.Key(encoding.EncodeUvarintAscending([]byte(sstDescsPrefix), uint64(id)))
}

func deprefix(key roachpb.Key, prefix string) (roachpb.Key, error) {
	if !bytes.HasPrefix(key, []byte(prefix)) {
		return nil, errors.Errorf("malformed key missing expected prefix %s: %q", prefix, key)
	}
	return key[len(prefix):], nil
}

func decodeDescSSTKey(key roachpb.Key) (descpb.ID, error) {
	key, err := deprefix(key, sstDescsPrefix)
	if err != nil {
		return 0, err
	}
	_, id, err := encoding.DecodeUvarintAscending(key)
	return descpb.ID(id), err
}

var _ bulk.Iterator[*backuppb.BackupManifest_DescriptorRevision] = &sliceIterator[backuppb.BackupManifest_DescriptorRevision]{}

// DescriptorRevisionIterator is a simple iterator to iterate over backuppb.BackupManifest_DescriptorRevisions.
type DescriptorRevisionIterator struct {
	backing bytesIter
	err     error
	value   *backuppb.BackupManifest_DescriptorRevision
}

func (dri *DescriptorRevisionIterator) Valid() (bool, error) {
	if dri.err != nil {
		return false, dri.err
	}
	return dri.value != nil, nil
}

// Value implements the Iterator interface.
func (dri *DescriptorRevisionIterator) Value() *backuppb.BackupManifest_DescriptorRevision {
	return dri.value
}

// Close closes the iterator.
func (dri *DescriptorRevisionIterator) Close() {
	dri.backing.close()
}

// Next retrieves the next descriptor revision in the iterator.
//
// Next returns true if next element was successfully unmarshalled into
// revision, and false if there are no more elements or if an error was
// encountered. When Next returns false, the user should call the Err method to
// verify the existence of an error.
func (dri *DescriptorRevisionIterator) Next() {
	if dri.err != nil {
		return
	}

	wrapper := resultWrapper{}
	ok := dri.backing.next(&wrapper)
	if !ok {
		if err := dri.backing.err(); err != nil {
			dri.err = err
		}

		dri.value = nil
		return
	}

	nextRev, err := unmarshalWrapper(&wrapper)
	if err != nil {
		dri.err = err
		return
	}

	dri.value = &nextRev
}

func unmarshalWrapper(wrapper *resultWrapper) (backuppb.BackupManifest_DescriptorRevision, error) {
	var desc *descpb.Descriptor
	if len(wrapper.value) > 0 {
		desc = &descpb.Descriptor{}
		err := protoutil.Unmarshal(wrapper.value, desc)
		if err != nil {
			return backuppb.BackupManifest_DescriptorRevision{}, err
		}
	}

	id, err := decodeDescSSTKey(wrapper.key.Key)
	if err != nil {
		return backuppb.BackupManifest_DescriptorRevision{}, err
	}

	rev := backuppb.BackupManifest_DescriptorRevision{
		Desc: desc,
		ID:   id,
		Time: wrapper.key.Timestamp,
	}
	return rev, nil
}

// NewDescIter creates a new Iterator over Descriptors.
func (f *IterFactory) NewDescIter(ctx context.Context) bulk.Iterator[*descpb.Descriptor] {
	if f.m.HasExternalManifestSSTs {
		backing := makeBytesIter(ctx, f.store, f.descriptorSSTPath, []byte(sstDescsPrefix), f.encryption, true, f.kmsEnv)
		it := DescIterator{
			backing: backing,
		}
		it.Next()
		return &it
	}

	return newSlicePointerIterator(f.m.Descriptors)
}

// Close closes the iterator.
func (di *DescIterator) Close() {
	di.backing.close()
}

// Valid implements the Iterator interface.
func (di *DescIterator) Valid() (bool, error) {
	if di.err != nil {
		return false, di.err
	}
	return di.value != nil, nil
}

// Value implements the Iterator interface.
func (di *DescIterator) Value() *descpb.Descriptor {
	return di.value
}

// Next implements the Iterator interface.
func (di *DescIterator) Next() {
	if di.err != nil {
		return
	}

	wrapper := resultWrapper{}
	var nextValue *descpb.Descriptor
	descHolder := descpb.Descriptor{}
	for di.backing.next(&wrapper) {
		err := protoutil.Unmarshal(wrapper.value, &descHolder)
		if err != nil {
			di.err = err
			return
		}

		tbl, db, typ, sc, fn := descpb.GetDescriptors(&descHolder)
		if tbl != nil || db != nil || typ != nil || sc != nil || fn != nil {
			nextValue = &descHolder
			break
		}
	}

	di.value = nextValue
}

// DescIterator is a simple iterator to iterate over descpb.Descriptors.
type DescIterator struct {
	backing bytesIter
	value   *descpb.Descriptor
	err     error
}

// NewDescriptorChangesIter creates a new Iterator over
// BackupManifest_DescriptorRevisions. It is assumed that descriptor changes are
// sorted by DescChangesLess.
func (f *IterFactory) NewDescriptorChangesIter(
	ctx context.Context,
) bulk.Iterator[*backuppb.BackupManifest_DescriptorRevision] {
	if f.m.HasExternalManifestSSTs {
		if f.m.MVCCFilter == backuppb.MVCCFilter_Latest {
			// If the manifest is backuppb.MVCCFilter_Latest, then return an empty
			// iterator for descriptor changes.
			var backing []backuppb.BackupManifest_DescriptorRevision
			return newSlicePointerIterator(backing)
		}

		backing := makeBytesIter(ctx, f.store, f.descriptorSSTPath, []byte(sstDescsPrefix), f.encryption,
			false, f.kmsEnv)
		dri := DescriptorRevisionIterator{
			backing: backing,
		}

		dri.Next()
		return &dri
	}

	return newSlicePointerIterator(f.m.DescriptorChanges)
}
