// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupinfo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// MetadataSSTName is the name of the SST file containing the backup metadata.
	MetadataSSTName = "metadata.sst"
	// BackupMetadataFilesListPath is the name of the SST file containing the
	// BackupManifest_Files of the backup. This file is always written in
	// conjunction with the `BACKUP_METADATA`.
	BackupMetadataFilesListPath = "filelist.sst"
	// FileInfoPath is the name of the SST file containing the
	// BackupManifest_Files of the backup.
	FileInfoPath     = "fileinfo.sst"
	sstBackupKey     = "backup"
	sstDescsPrefix   = "desc/"
	sstFilesPrefix   = "file/"
	sstNamesPrefix   = "name/"
	sstSpansPrefix   = "span/"
	sstStatsPrefix   = "stats/"
	sstTenantsPrefix = "tenant/"
)

var iterOpts = storage.IterOptions{
	KeyTypes:   storage.IterKeyTypePointsOnly,
	LowerBound: keys.LocalMax,
	UpperBound: keys.MaxKey,
}

// WriteFilesListSST is responsible for constructing and writing the
// filePathInfo to dest. This file contains the `BackupManifest_Files` of the
// backup.
func WriteFilesListSST(
	ctx context.Context,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifest *backuppb.BackupManifest,
	filePathInfo string,
) error {
	return writeFilesSST(ctx, manifest, dest, enc, kmsEnv, filePathInfo)
}

// WriteBackupMetadataSST is responsible for constructing and writing the
// `metadata.sst` to dest. This file contains the metadata corresponding to this
// backup.
func WriteBackupMetadataSST(
	ctx context.Context,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	manifest *backuppb.BackupManifest,
	stats []*stats.TableStatisticProto,
) error {
	var w io.WriteCloser
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel() // cancel before Close() to abort write on err returns.
		if w != nil {
			w.Close()
		}
	}()

	w, err := makeWriter(ctx, dest, MetadataSSTName, enc, kmsEnv)
	if err != nil {
		return err
	}

	if err := constructMetadataSST(ctx, dest, enc, kmsEnv, w, manifest, stats); err != nil {
		return err
	}

	// Explicitly close to flush and check for errors do so before defer's cancel
	// which would abort. Then nil out w to avoid defer double-closing.
	err = w.Close()
	w = nil
	return err
}

func makeWriter(
	ctx context.Context,
	dest cloud.ExternalStorage,
	filename string,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (io.WriteCloser, error) {
	w, err := dest.Writer(ctx, filename)
	if err != nil {
		return nil, err
	}

	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return nil, err
		}
		encW, err := storageccl.EncryptingWriter(w, key)
		if err != nil {
			return nil, err
		}
		w = encW
	}
	return w, nil
}

func constructMetadataSST(
	ctx context.Context,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	w io.Writer,
	m *backuppb.BackupManifest,
	stats []*stats.TableStatisticProto,
) error {
	// TODO(dt): use a seek-optimized SST writer instead.
	sst := storage.MakeBackupSSTWriter(ctx, dest.Settings(), w)
	defer sst.Close()

	// The following steps must be done in-order, by key prefix.

	if err := writeManifestToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeDescsToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeFilesToMetadata(ctx, sst, m, dest, enc, kmsEnv, FileInfoPath); err != nil {
		return err
	}

	if err := writeNamesToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeSpansToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeStatsToMetadata(ctx, sst, stats); err != nil {
		return err
	}

	if err := writeTenantsToMetadata(ctx, sst, m); err != nil {
		return err
	}

	return sst.Finish()
}

func writeManifestToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	info := *m
	info.Descriptors = nil
	info.DescriptorChanges = nil
	info.Files = nil
	info.Spans = nil
	info.StatisticsFilenames = nil
	info.IntroducedSpans = nil
	info.Tenants = nil

	b, err := protoutil.Marshal(&info)
	if err != nil {
		return err
	}
	return sst.PutUnversioned(roachpb.Key(sstBackupKey), b)
}

func writeDescsToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	// Add descriptors from revisions if available, Descriptors if not.
	if len(m.DescriptorChanges) > 0 {
		sort.Slice(m.DescriptorChanges, func(i, j int) bool {
			if m.DescriptorChanges[i].ID < m.DescriptorChanges[j].ID {
				return true
			} else if m.DescriptorChanges[i].ID == m.DescriptorChanges[j].ID {
				return !m.DescriptorChanges[i].Time.Less(m.DescriptorChanges[j].Time)
			}
			return false
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
			if m.StartTime.IsEmpty() {
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

func FileCmp(left backuppb.BackupManifest_File, right backuppb.BackupManifest_File) int {
	if cmp := left.Span.Key.Compare(right.Span.Key); cmp != 0 {
		return cmp
	}

	return strings.Compare(left.Path, right.Path)
}

func writeFilesSST(
	ctx context.Context,
	m *backuppb.BackupManifest,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	fileInfoPath string,
) error {
	w, err := makeWriter(ctx, dest, fileInfoPath, enc, kmsEnv)
	if err != nil {
		return err
	}
	defer w.Close()
	fileSST := storage.MakeBackupSSTWriter(ctx, dest.Settings(), w)
	defer fileSST.Close()

	// Sort and write all of the files into a single file info SST.
	sort.Slice(m.Files, func(i, j int) bool {
		return FileCmp(m.Files[i], m.Files[j]) < 0
	})

	for i := range m.Files {
		file := m.Files[i]
		b, err := protoutil.Marshal(&file)
		if err != nil {
			return err
		}
		if err := fileSST.PutUnversioned(encodeFileSSTKey(file.Span.Key, file.Path), b); err != nil {
			return err
		}
	}

	err = fileSST.Finish()
	if err != nil {
		return err
	}
	return w.Close()
}

func writeFilesToMetadata(
	ctx context.Context,
	sst storage.SSTWriter,
	m *backuppb.BackupManifest,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	fileInfoPath string,
) error {
	if err := writeFilesSST(ctx, m, dest, enc, kmsEnv, fileInfoPath); err != nil {
		return err
	}
	// Write the file info into the main metadata SST.
	return sst.PutUnversioned(encodeFilenameSSTKey(fileInfoPath), nil)
}

type name struct {
	parent, parentSchema descpb.ID
	name                 string
	id                   descpb.ID
	ts                   hlc.Timestamp
}

type namespace []name

func (a namespace) Len() int      { return len(a) }
func (a namespace) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a namespace) Less(i, j int) bool {
	if a[i].parent == a[j].parent {
		if a[i].parentSchema == a[j].parentSchema {
			cmpName := strings.Compare(a[i].name, a[j].name)
			if cmpName == 0 {
				cmpTimestamp := a[j].ts.Compare(a[i].ts)
				if cmpTimestamp == 0 {
					return a[i].id > a[j].id
				}
				if a[i].ts.IsEmpty() {
					return true
				}
				return cmpTimestamp < 0
			}
			return cmpName < 0
		}
		return a[i].parentSchema < a[j].parentSchema
	}
	return a[i].parent < a[j].parent
}

func writeNamesToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	revs := m.DescriptorChanges
	if len(revs) == 0 {
		revs = make([]backuppb.BackupManifest_DescriptorRevision, len(m.Descriptors))
		for i := range m.Descriptors {
			revs[i].Desc = &m.Descriptors[i]
			revs[i].Time = m.EndTime
			revs[i].ID = descID(m.Descriptors[i])
		}
	}

	names := make(namespace, len(revs))

	for i, rev := range revs {
		names[i].id = rev.ID
		names[i].ts = rev.Time
		tb, db, typ, sc, fn := descpb.GetDescriptors(rev.Desc)
		if db != nil {
			names[i].name = db.Name
			if db.State == descpb.DescriptorState_DROP {
				names[i].id = 0
			}
		} else if sc != nil {
			names[i].name = sc.Name
			names[i].parent = sc.ParentID
			if sc.State == descpb.DescriptorState_DROP {
				names[i].id = 0
			}
		} else if tb != nil {
			names[i].name = tb.Name
			names[i].parent = tb.ParentID
			names[i].parentSchema = keys.PublicSchemaID
			if s := tb.UnexposedParentSchemaID; s != descpb.InvalidID {
				names[i].parentSchema = s
			}
			if tb.State == descpb.DescriptorState_DROP {
				names[i].id = 0
			}
		} else if typ != nil {
			names[i].name = typ.Name
			names[i].parent = typ.ParentID
			names[i].parentSchema = typ.ParentSchemaID
			if typ.State == descpb.DescriptorState_DROP {
				names[i].id = 0
			}
		} else if fn != nil {
			names[i].name = fn.Name
			names[i].parent = fn.ParentID
			names[i].parentSchema = fn.ParentSchemaID
			if fn.State == descpb.DescriptorState_DROP {
				names[i].id = 0
			}
		}
	}
	sort.Sort(names)

	for i, rev := range names {
		if rev.name == "" {
			continue
		}
		if i > 0 {
			prev := names[i-1]
			prev.id = rev.id
			if prev == rev {
				// Name has multiple ID mappings at the same timestamp.
				// At most one of these can be non-zero. Zero IDs correspond to name
				// entry deletions following a DROP. If there is a non-zero ID, it means
				// that the DROP was followed by a RENAME or a CREATE using the same
				// name.
				// The sort ordering of the names guarantees that the zero IDs are last,
				// we therefore keep only the first mapping.
				if rev.id != 0 {
					return errors.AssertionFailedf(
						"attempting to write duplicate name mappings for key (%d, %d, %q) at %s: IDs %d and %d",
						rev.parent, rev.parentSchema, rev.name, rev.ts, prev.id, rev.id)
				}
				continue
			}
			prev = names[i-1]
			prev.ts = rev.ts
			if prev == rev {
				// Ignore identical mappings at subsequent timestamps.
				continue
			}
		}
		k := encodeNameSSTKey(rev.parent, rev.parentSchema, rev.name)
		v := encoding.EncodeUvarintAscending(nil, uint64(rev.id))
		if err := sst.PutRawMVCC(storage.MVCCKey{Key: k, Timestamp: rev.ts}, v); err != nil {
			return err
		}
	}

	return nil
}

func writeSpansToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	sort.Sort(roachpb.Spans(m.Spans))
	sort.Sort(roachpb.Spans(m.IntroducedSpans))

	for i, j := 0, 0; i < len(m.Spans) || j < len(m.IntroducedSpans); {
		var sp roachpb.Span
		var ts hlc.Timestamp

		// Merge spans and introduced spans into one series of spans where the ts on
		// each is 0 if it was introduced or the backup start time otherwise.
		if j >= len(m.IntroducedSpans) {
			sp = m.Spans[i]
			ts = m.StartTime
			i++
		} else if i >= len(m.Spans) {
			sp = m.IntroducedSpans[j]
			ts = hlc.Timestamp{}
			j++
		} else {
			cmp := m.Spans[i].Key.Compare(m.IntroducedSpans[j].Key)
			if cmp < 0 {
				sp = m.Spans[i]
				ts = m.StartTime
				i++
			} else {
				sp = m.IntroducedSpans[j]
				ts = hlc.Timestamp{}
				j++
			}
		}
		if ts.IsEmpty() {
			if err := sst.PutUnversioned(encodeSpanSSTKey(sp), nil); err != nil {
				return err
			}
		} else {
			k := storage.MVCCKey{Key: encodeSpanSSTKey(sp), Timestamp: ts}
			if err := sst.PutRawMVCC(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeStatsToMetadata(
	ctx context.Context, sst storage.SSTWriter, stats []*stats.TableStatisticProto,
) error {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].TableID < stats[j].TableID || (stats[i].TableID == stats[j].TableID && stats[i].StatisticID < stats[j].StatisticID)
	})

	for _, s := range stats {
		b, err := protoutil.Marshal(s)
		if err != nil {
			return err
		}
		if err := sst.PutUnversioned(encodeStatSSTKey(s.TableID, s.StatisticID), b); err != nil {
			return err
		}
	}
	return nil
}

func writeTenantsToMetadata(
	ctx context.Context, sst storage.SSTWriter, m *backuppb.BackupManifest,
) error {
	sort.Slice(m.Tenants, func(i, j int) bool { return m.Tenants[i].ID < m.Tenants[j].ID })
	for _, i := range m.Tenants {
		b, err := protoutil.Marshal(&i)
		if err != nil {
			return err
		}
		if err := sst.PutUnversioned(encodeTenantSSTKey(i.ID), b); err != nil {
			return err
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

func deprefix(key roachpb.Key, prefix string) (roachpb.Key, error) {
	if !bytes.HasPrefix(key, []byte(prefix)) {
		return nil, errors.Errorf("malformed key missing expected prefix %s: %q", prefix, key)
	}
	return key[len(prefix):], nil
}

func encodeDescSSTKey(id descpb.ID) roachpb.Key {
	return roachpb.Key(encoding.EncodeUvarintAscending([]byte(sstDescsPrefix), uint64(id)))
}

func decodeDescSSTKey(key roachpb.Key) (descpb.ID, error) {
	key, err := deprefix(key, sstDescsPrefix)
	if err != nil {
		return 0, err
	}
	_, id, err := encoding.DecodeUvarintAscending(key)
	return descpb.ID(id), err
}

func encodeFileSSTKey(spanStart roachpb.Key, filename string) roachpb.Key {
	buf := make([]byte, 0)
	buf = encoding.EncodeBytesAscending(buf, spanStart)
	return roachpb.Key(encoding.EncodeStringAscending(buf, filename))
}

func encodeFilenameSSTKey(filename string) roachpb.Key {
	return encoding.EncodeStringAscending([]byte(sstFilesPrefix), filename)
}

func decodeUnsafeFileSSTKey(key roachpb.Key) (roachpb.Key, string, error) {
	key, spanStart, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return nil, "", err
	}
	_, filename, err := encoding.DecodeUnsafeStringAscending(key, nil)
	if err != nil {
		return nil, "", err
	}
	return roachpb.Key(spanStart), filename, err
}

func decodeUnsafeFileInfoSSTKey(key roachpb.Key) (string, error) {
	key, err := deprefix(key, sstFilesPrefix)
	if err != nil {
		return "", err
	}

	_, path, err := encoding.DecodeUnsafeStringAscending(key, nil)
	if err != nil {
		return "", err
	}
	return path, err
}

func encodeNameSSTKey(parentDB, parentSchema descpb.ID, name string) roachpb.Key {
	buf := []byte(sstNamesPrefix)
	buf = encoding.EncodeUvarintAscending(buf, uint64(parentDB))
	buf = encoding.EncodeUvarintAscending(buf, uint64(parentSchema))
	return roachpb.Key(encoding.EncodeStringAscending(buf, name))
}

func decodeUnsafeNameSSTKey(key roachpb.Key) (descpb.ID, descpb.ID, string, error) {
	key, err := deprefix(key, sstNamesPrefix)
	if err != nil {
		return 0, 0, "", err
	}
	key, parentID, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, 0, "", err
	}
	key, schemaID, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, 0, "", err
	}
	_, name, err := encoding.DecodeUnsafeStringAscending(key, nil)
	if err != nil {
		return 0, 0, "", err
	}
	return descpb.ID(parentID), descpb.ID(schemaID), name, nil
}

func encodeSpanSSTKey(span roachpb.Span) roachpb.Key {
	buf := encoding.EncodeBytesAscending([]byte(sstSpansPrefix), span.Key)
	return roachpb.Key(encoding.EncodeBytesAscending(buf, span.EndKey))
}

func decodeSpanSSTKey(key roachpb.Key) (roachpb.Span, error) {
	key, err := deprefix(key, sstSpansPrefix)
	if err != nil {
		return roachpb.Span{}, err
	}
	key, start, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return roachpb.Span{}, err
	}
	_, end, err := encoding.DecodeBytesAscending(key, nil)
	return roachpb.Span{Key: start, EndKey: end}, err
}

func encodeStatSSTKey(id descpb.ID, statID uint64) roachpb.Key {
	buf := encoding.EncodeUvarintAscending([]byte(sstStatsPrefix), uint64(id))
	return roachpb.Key(encoding.EncodeUvarintAscending(buf, statID))
}

func decodeStatSSTKey(key roachpb.Key) (descpb.ID, uint64, error) {
	key, err := deprefix(key, sstStatsPrefix)
	if err != nil {
		return 0, 0, err
	}
	key, id, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, 0, err
	}
	_, stat, err := encoding.DecodeUvarintAscending(key)
	return descpb.ID(id), stat, err
}

func encodeTenantSSTKey(id uint64) roachpb.Key {
	return encoding.EncodeUvarintAscending([]byte(sstTenantsPrefix), id)
}

func decodeTenantSSTKey(key roachpb.Key) (uint64, error) {
	key, err := deprefix(key, sstTenantsPrefix)
	if err != nil {
		return 0, err
	}
	_, id, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func pbBytesToJSON(in []byte, msg protoutil.Message) (json.JSON, error) {
	if err := protoutil.Unmarshal(in, msg); err != nil {
		return nil, err
	}
	j, err := protoreflect.MessageToJSON(msg, protoreflect.FmtFlags{})
	if err != nil {
		return nil, err
	}
	return j, nil
}

func debugDumpFileSST(
	ctx context.Context,
	store cloud.ExternalStorage,
	fileInfoPath string,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	out func(rawKey, readableKey string, value json.JSON) error,
) error {
	var encOpts *roachpb.FileEncryptionOptions
	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return err
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}
	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: store, FilePath: fileInfoPath}}, encOpts, iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.SeekGE(storage.MVCCKey{}); ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		k := iter.UnsafeKey()
		spanStart, path, err := decodeUnsafeFileSSTKey(k.Key)
		if err != nil {
			return err
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			return err
		}
		f, err := pbBytesToJSON(v, &backuppb.BackupManifest_File{})
		if err != nil {
			return err
		}
		if err := out(k.String(), fmt.Sprintf("file %s (%s)", path, spanStart.String()), f); err != nil {
			return err
		}
	}

	return nil
}

// DebugDumpMetadataSST is for debugging a metadata SST.
func DebugDumpMetadataSST(
	ctx context.Context,
	store cloud.ExternalStorage,
	path string,
	enc *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	out func(rawKey, readableKey string, value json.JSON) error,
) error {
	var encOpts *roachpb.FileEncryptionOptions
	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return err
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}
	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: store,
		FilePath: path}}, encOpts, iterOpts)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(storage.MVCCKey{}); ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		k := iter.UnsafeKey()
		switch {
		case bytes.Equal(k.Key, []byte(sstBackupKey)):
			v, err := iter.UnsafeValue()
			if err != nil {
				return err
			}
			info, err := pbBytesToJSON(v, &backuppb.BackupManifest{})
			if err != nil {
				return err
			}
			if err := out(k.String(), "backup info", info); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstDescsPrefix)):
			id, err := decodeDescSSTKey(k.Key)
			if err != nil {
				return err
			}
			var desc json.JSON
			if v, err := iter.UnsafeValue(); err == nil && len(v) > 0 {
				desc, err = pbBytesToJSON(v, &descpb.Descriptor{})
				if err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("desc %d @ %v", id, k.Timestamp), desc); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstFilesPrefix)):
			p, err := decodeUnsafeFileInfoSSTKey(k.Key)
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("file info @ %s", p), nil); err != nil {
				return err
			}
			if err := debugDumpFileSST(ctx, store, p, enc, kmsEnv, out); err != nil {
				return err
			}
		case bytes.HasPrefix(k.Key, []byte(sstNamesPrefix)):
			db, sc, name, err := decodeUnsafeNameSSTKey(k.Key)
			if err != nil {
				return err
			}
			var id uint64
			if v, err := iter.UnsafeValue(); err == nil && len(v) > 0 {
				_, id, err = encoding.DecodeUvarintAscending(v)
				if err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			mapping := fmt.Sprintf("name db %d / schema %d / %q @ %v -> %d", db, sc, name, k.Timestamp, id)
			if err := out(k.String(), mapping, nil); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstSpansPrefix)):
			span, err := decodeSpanSSTKey(k.Key)
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("span %s @ %v", span, k.Timestamp), nil); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstStatsPrefix)):
			tblID, statID, err := decodeStatSSTKey(k.Key)
			if err != nil {
				return err
			}
			v, err := iter.UnsafeValue()
			if err != nil {
				return err
			}
			s, err := pbBytesToJSON(v, &stats.TableStatisticProto{})
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("stats tbl %d, id %d", tblID, statID), s); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstTenantsPrefix)):
			id, err := decodeTenantSSTKey(k.Key)
			if err != nil {
				return err
			}
			v, err := iter.UnsafeValue()
			if err != nil {
				return err
			}
			i, err := pbBytesToJSON(v, &mtinfopb.ProtoInfo{})
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("tenant %d", id), i); err != nil {
				return err
			}

		default:
			v, err := iter.UnsafeValue()
			if err != nil {
				return err
			}
			if err := out(k.String(), "unknown", json.FromString(fmt.Sprintf("%q", v))); err != nil {
				return err
			}
		}
	}

	return nil
}

// BackupMetadata holds all of the data in backuppb.BackupManifest except a few repeated
// fields such as descriptors or spans. BackupMetadata provides iterator methods
// so that the excluded fields can be accessed in a streaming manner.
type BackupMetadata struct {
	backuppb.BackupManifest
	store    cloud.ExternalStorage
	enc      *jobspb.BackupEncryptionOptions
	filename string
	kmsEnv   cloud.KMSEnv
}

// NewBackupMetadata returns a new BackupMetadata instance.
func NewBackupMetadata(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	sstFileName string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) (*BackupMetadata, error) {
	var encOpts *roachpb.FileEncryptionOptions
	if encryption != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, encryption, kmsEnv)
		if err != nil {
			return nil, err
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}
	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: exportStore,
		FilePath: sstFileName}}, encOpts, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var sstManifest backuppb.BackupManifest
	iter.SeekGE(storage.MakeMVCCMetadataKey([]byte(sstBackupKey)))
	ok, err := iter.Valid()
	if err != nil {
		return nil, err
	}
	if !ok || !iter.UnsafeKey().Key.Equal([]byte(sstBackupKey)) {
		return nil, errors.Errorf("metadata SST does not contain backup manifest")
	}

	v, err := iter.UnsafeValue()
	if err != nil {
		return nil, err
	}
	if err := protoutil.Unmarshal(v, &sstManifest); err != nil {
		return nil, err
	}

	return &BackupMetadata{BackupManifest: sstManifest, store: exportStore,
		enc: encryption, filename: sstFileName, kmsEnv: kmsEnv}, nil
}

// SpanIterator is a simple iterator to iterate over roachpb.Spans.
type SpanIterator struct {
	backing bytesIter
	filter  func(key storage.MVCCKey) bool
	err     error
}

// SpanIter creates a new SpanIterator for the backup metadata.
func (b *BackupMetadata) SpanIter(ctx context.Context) SpanIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstSpansPrefix), b.enc,
		true, b.kmsEnv)
	return SpanIterator{
		backing: backing,
	}
}

// IntroducedSpanIter creates a new IntroducedSpanIterator for the backup metadata.
func (b *BackupMetadata) IntroducedSpanIter(ctx context.Context) SpanIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstSpansPrefix), b.enc,
		false, b.kmsEnv)

	return SpanIterator{
		backing: backing,
		filter: func(key storage.MVCCKey) bool {
			return key.Timestamp == hlc.Timestamp{}
		},
	}
}

// Close closes the iterator.
func (si *SpanIterator) Close() {
	si.backing.close()
}

// Err returns the iterator's error
func (si *SpanIterator) Err() error {
	if si.err != nil {
		return si.err
	}
	return si.backing.err()
}

// Next retrieves the next span in the iterator.
//
// Next returns true if next element was successfully unmarshalled into span,
// and false if there are no more elements or if an error was encountered. When
// Next returns false, the user should call the Err method to verify the
// existence of an error.
func (si *SpanIterator) Next(span *roachpb.Span) bool {
	wrapper := resultWrapper{}

	for si.backing.next(&wrapper) {
		if si.filter == nil || si.filter(wrapper.key) {
			sp, err := decodeSpanSSTKey(wrapper.key.Key)
			if err != nil {
				si.err = err
				return false
			}

			*span = sp
			return true
		}
	}

	return false
}

// FileIterator is a simple iterator to iterate over backuppb.BackupManifest_File.
type FileIterator struct {
	mergedIterator storage.SimpleMVCCIterator
	err            error
	file           *backuppb.BackupManifest_File
}

// NewFileIter creates a new FileIterator for the backup metadata.
func (b *BackupMetadata) NewFileIter(ctx context.Context) (*FileIterator, error) {
	fileInfoIter := makeBytesIter(ctx, b.store, b.filename, []byte(sstFilesPrefix), b.enc,
		false, b.kmsEnv)
	defer fileInfoIter.close()

	var storeFiles []storageccl.StoreFile
	var encOpts *roachpb.FileEncryptionOptions
	if b.enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, b.enc, b.kmsEnv)
		if err != nil {
			return nil, err
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}

	result := resultWrapper{}
	for fileInfoIter.next(&result) {
		path, err := decodeUnsafeFileInfoSSTKey(result.key.Key)
		if err != nil {
			break
		}
		storeFiles = append(storeFiles, storageccl.StoreFile{Store: b.store,
			FilePath: path})
	}

	if fileInfoIter.err() != nil {
		return nil, fileInfoIter.err()
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, encOpts, iterOpts)
	if err != nil {
		return nil, err
	}
	iter.SeekGE(storage.MVCCKey{})
	return &FileIterator{mergedIterator: iter}, nil
}

// NewFileSSTIter creates a new FileIterator to iterate over the storeFile.
// It is the caller's responsibility to Close() the returned iterator.
func NewFileSSTIter(
	ctx context.Context, storeFile storageccl.StoreFile, encOpts *roachpb.FileEncryptionOptions,
) (*FileIterator, error) {
	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{storeFile}, encOpts, iterOpts)
	if err != nil {
		return nil, err
	}
	iter.SeekGE(storage.MVCCKey{})
	return &FileIterator{mergedIterator: iter}, nil
}

// Close closes the iterator.
func (fi *FileIterator) Close() {
	fi.mergedIterator.Close()
}

// Valid indicates whether or not the iterator is pointing to a valid value.
func (fi *FileIterator) Valid() (bool, error) {
	if fi.err != nil {
		return false, fi.err
	}

	if ok, err := fi.mergedIterator.Valid(); !ok {
		fi.err = err
		return ok, err
	}

	if fi.file == nil {
		v, err := fi.mergedIterator.UnsafeValue()
		if err != nil {
			fi.err = err
			return false, fi.err
		}

		file := &backuppb.BackupManifest_File{}
		err = protoutil.Unmarshal(v, file)
		if err != nil {
			fi.err = err
			return false, fi.err
		}
		fi.file = file
	}
	return true, nil
}

// Value returns the current value of the iterator, if valid.
func (fi *FileIterator) Value() *backuppb.BackupManifest_File {
	return fi.file
}

// Next advances the iterator the the next value.
func (fi *FileIterator) Next() {
	fi.mergedIterator.Next()
	fi.file = nil
}

// Reset resets the iterator to the first value.
func (fi *FileIterator) Reset() {
	fi.mergedIterator.SeekGE(storage.MVCCKey{})
	fi.err = nil
	fi.file = nil
}

// DescIterator is a simple iterator to iterate over descpb.Descriptors.
type DescIterator struct {
	backing bytesIter
	err     error
}

// DescIter creates a new DescIterator for the backup metadata.
func (b *BackupMetadata) DescIter(ctx context.Context) DescIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstDescsPrefix), b.enc,
		true, b.kmsEnv)
	return DescIterator{
		backing: backing,
	}
}

// Close closes the iterator.
func (di *DescIterator) Close() {
	di.backing.close()
}

// Err returns the iterator's error.
func (di *DescIterator) Err() error {
	if di.err != nil {
		return di.err
	}
	return di.backing.err()
}

// Next retrieves the next descriptor in the iterator.
//
// Next returns true if next element was successfully unmarshalled into desc ,
// and false if there are no more elements or if an error was encountered. When
// Next returns false, the user should call the Err method to verify the
// existence of an error.
func (di *DescIterator) Next(desc *descpb.Descriptor) bool {
	wrapper := resultWrapper{}

	for di.backing.next(&wrapper) {
		err := protoutil.Unmarshal(wrapper.value, desc)
		if err != nil {
			di.err = err
			return false
		}

		tbl, db, typ, sc, fn := descpb.GetDescriptors(desc)
		if tbl != nil || db != nil || typ != nil || sc != nil || fn != nil {
			return true
		}
	}

	return false
}

// TenantIterator is a simple iterator to iterate over TenantInfoWithUsages.
type TenantIterator struct {
	backing bytesIter
	err     error
}

// TenantIter creates a new TenantIterator for the backup metadata.
func (b *BackupMetadata) TenantIter(ctx context.Context) TenantIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstTenantsPrefix), b.enc,
		false, b.kmsEnv)
	return TenantIterator{
		backing: backing,
	}
}

// Close closes the iterator.
func (ti *TenantIterator) Close() {
	ti.backing.close()
}

// Err returns the iterator's error.
func (ti *TenantIterator) Err() error {
	if ti.err != nil {
		return ti.err
	}
	return ti.backing.err()
}

// Next retrieves the next tenant in the iterator.
//
// Next returns true if next element was successfully unmarshalled into tenant,
// and false if there are no more elements or if an error was encountered. When
// Next returns false, the user should call the Err method to verify the
// existence of an error.
func (ti *TenantIterator) Next(tenant *mtinfopb.TenantInfoWithUsage) bool {
	wrapper := resultWrapper{}
	ok := ti.backing.next(&wrapper)
	if !ok {
		return false
	}

	err := protoutil.Unmarshal(wrapper.value, tenant)
	if err != nil {
		ti.err = err
		return false
	}

	return true
}

// DescriptorRevisionIterator is a simple iterator to iterate over backuppb.BackupManifest_DescriptorRevisions.
type DescriptorRevisionIterator struct {
	backing bytesIter
	err     error
}

// DescriptorChangesIter creates a new DescriptorChangesIterator for the backup metadata.
func (b *BackupMetadata) DescriptorChangesIter(ctx context.Context) DescriptorRevisionIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstDescsPrefix), b.enc,
		false, b.kmsEnv)
	return DescriptorRevisionIterator{
		backing: backing,
	}
}

// Close closes the iterator.
func (dri *DescriptorRevisionIterator) Close() {
	dri.backing.close()
}

// Err returns the iterator's error.
func (dri *DescriptorRevisionIterator) Err() error {
	if dri.err != nil {
		return dri.err
	}
	return dri.backing.err()
}

// Next retrieves the next descriptor revision in the iterator.
//
// Next returns true if next element was successfully unmarshalled into
// revision, and false if there are no more elements or if an error was
// encountered. When Next returns false, the user should call the Err method to
// verify the existence of an error.
func (dri *DescriptorRevisionIterator) Next(
	revision *backuppb.BackupManifest_DescriptorRevision,
) bool {
	wrapper := resultWrapper{}
	ok := dri.backing.next(&wrapper)
	if !ok {
		return false
	}

	err := unmarshalWrapper(&wrapper, revision)
	if err != nil {
		dri.err = err
		return false
	}

	return true
}

func unmarshalWrapper(
	wrapper *resultWrapper, rev *backuppb.BackupManifest_DescriptorRevision,
) error {
	var desc *descpb.Descriptor
	if len(wrapper.value) > 0 {
		desc = &descpb.Descriptor{}
		err := protoutil.Unmarshal(wrapper.value, desc)
		if err != nil {
			return err
		}
	}

	id, err := decodeDescSSTKey(wrapper.key.Key)
	if err != nil {
		return err
	}

	*rev = backuppb.BackupManifest_DescriptorRevision{
		Desc: desc,
		ID:   id,
		Time: wrapper.key.Timestamp,
	}
	return nil
}

// StatsIterator is a simple iterator to iterate over stats.TableStatisticProtos.
type StatsIterator struct {
	backing bytesIter
	err     error
}

// StatsIter creates a new StatsIterator for the backup metadata.
func (b *BackupMetadata) StatsIter(ctx context.Context) StatsIterator {
	backing := makeBytesIter(ctx, b.store, b.filename, []byte(sstStatsPrefix), b.enc,
		false, b.kmsEnv)
	return StatsIterator{
		backing: backing,
	}
}

// Close closes the iterator.
func (si *StatsIterator) Close() {
	si.backing.close()
}

// Err returns the iterator's error.
func (si *StatsIterator) Err() error {
	if si.err != nil {
		return si.err
	}
	return si.backing.err()
}

// Next retrieves the next stats proto in the iterator.
//
// Next returns true if next element was successfully unmarshalled into
// statsPtr, and false if there are no more elements or if an error was
// encountered. When Next returns false, the user should call the Err method to verify the
// existence of an error.
func (si *StatsIterator) Next(statsPtr **stats.TableStatisticProto) bool {
	wrapper := resultWrapper{}
	ok := si.backing.next(&wrapper)

	if !ok {
		return false
	}

	var s stats.TableStatisticProto
	err := protoutil.Unmarshal(wrapper.value, &s)
	if err != nil {
		si.err = err
		return false
	}

	*statsPtr = &s
	return true
}

type bytesIter struct {
	Iter storage.SimpleMVCCIterator

	prefix      []byte
	useMVCCNext bool
	iterError   error
}

func makeBytesIter(
	ctx context.Context,
	store cloud.ExternalStorage,
	path string,
	prefix []byte,
	enc *jobspb.BackupEncryptionOptions,
	useMVCCNext bool,
	kmsEnv cloud.KMSEnv,
) bytesIter {
	var encOpts *roachpb.FileEncryptionOptions
	if enc != nil {
		key, err := backupencryption.GetEncryptionKey(ctx, enc, kmsEnv)
		if err != nil {
			return bytesIter{iterError: err}
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}

	iter, err := storageccl.ExternalSSTReader(ctx, []storageccl.StoreFile{{Store: store,
		FilePath: path}}, encOpts, iterOpts)
	if err != nil {
		return bytesIter{iterError: err}
	}

	iter.SeekGE(storage.MakeMVCCMetadataKey(prefix))
	return bytesIter{
		Iter:        iter,
		prefix:      prefix,
		useMVCCNext: useMVCCNext,
	}
}

func (bi *bytesIter) next(resWrapper *resultWrapper) bool {
	if bi.iterError != nil {
		return false
	}

	valid, err := bi.Iter.Valid()
	if err != nil || !valid || !bytes.HasPrefix(bi.Iter.UnsafeKey().Key, bi.prefix) {
		bi.close()
		bi.iterError = err
		return false
	}

	key := bi.Iter.UnsafeKey()
	resWrapper.key.Key = key.Key.Clone()
	resWrapper.key.Timestamp = key.Timestamp
	resWrapper.value = resWrapper.value[:0]
	v, err := bi.Iter.UnsafeValue()
	if err != nil {
		bi.close()
		bi.iterError = err
		return false
	}
	resWrapper.value = append(resWrapper.value, v...)

	if bi.useMVCCNext {
		bi.Iter.NextKey()
	} else {
		bi.Iter.Next()
	}
	return true
}

func (bi *bytesIter) err() error {
	return bi.iterError
}

func (bi *bytesIter) close() {
	if bi.Iter != nil {
		bi.Iter.Close()
		bi.Iter = nil
	}
}

type resultWrapper struct {
	key   storage.MVCCKey
	value []byte
}
