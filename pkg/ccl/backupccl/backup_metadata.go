// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	metadataSSTName = "metadata.sst"

	sstBackupKey     = "backup"
	sstDescsPrefix   = "desc/"
	sstFilesPrefix   = "file/"
	sstNamesPrefix   = "name/"
	sstSpansPrefix   = "span/"
	sstStatsPrefix   = "stats/"
	sstTenantsPrefix = "tenant/"
)

func writeBackupMetadataSST(
	ctx context.Context,
	dest cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	manifest *BackupManifest,
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

	w, err := dest.Writer(ctx, metadataSSTName)
	if err != nil {
		return err
	}

	if enc != nil {
		key, err := getEncryptionKey(ctx, enc, dest.Settings(), dest.ExternalIOConf())
		if err != nil {
			return err
		}
		encW, err := storageccl.EncryptingWriter(w, key)
		if err != nil {
			return err
		}
		w = encW
	}

	if err := constructMetadataSST(ctx, w, manifest, stats); err != nil {
		return err
	}

	// Explicitly close to flush and check for errors do so before defer's cancel
	// which would abort. Then nil out w to avoid defer double-closing.
	err = w.Close()
	w = nil
	return err
}

func constructMetadataSST(ctx context.Context,
	w io.Writer,
	m *BackupManifest,
	stats []*stats.TableStatisticProto,
) error {
	// TODO(dt): use a seek-optimized SST writer instead.
	sst := storage.MakeBackupSSTWriter(w)
	defer sst.Close()

	// The following steps must be done in-order, by key prefix.

	if err := writeManifestToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeDescsToMetadata(ctx, sst, m); err != nil {
		return err
	}

	if err := writeFilesToMetadata(ctx, sst, m); err != nil {
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

func writeManifestToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
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

func writeDescsToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
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
				if t := i.Desc.GetTable(); t == nil || !t.Dropped() {
					bytes, err := protoutil.Marshal(i.Desc)
					if err != nil {
						return err
					}
					b = bytes
				}
			}
			sst.PutMVCC(storage.MVCCKey{Key: k, Timestamp: i.Time}, b)
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
			// It does not really matter what time we put the descriptors at for a
			// non-rev-history backup -- we could put them at 0 or at end-time, since
			// any RESTORE will be reading as of end-time which would be >= both.
			sst.PutMVCC(storage.MVCCKey{Key: k, Timestamp: m.EndTime}, b)
		}
	}
	return nil
}

func writeFilesToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
	sort.Slice(m.Files, func(i, j int) bool {
		cmp := m.Files[i].Span.Key.Compare(m.Files[j].Span.Key)
		return cmp < 0 || (cmp == 0 && strings.Compare(m.Files[i].Path, m.Files[j].Path) < 0)
	})

	for _, i := range m.Files {
		b, err := protoutil.Marshal(&i)
		if err != nil {
			return err
		}
		if err := sst.PutUnversioned(encodeFileSSTKey(i.Span.Key, i.Path), b); err != nil {
			return err
		}
	}
	return nil
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
			cmp := strings.Compare(a[i].name, a[j].name)
			return cmp < 0 || (cmp == 0 && (a[i].ts.IsEmpty() || a[j].ts.Less(a[i].ts)))
		}
		return a[i].parentSchema < a[j].parentSchema
	}
	return a[i].parent < a[j].parent
}

func writeNamesToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
	revs := m.DescriptorChanges
	if len(revs) == 0 {
		revs := make([]BackupManifest_DescriptorRevision, len(m.Descriptors))
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
		if db := rev.Desc.GetDatabase(); db != nil {
			names[i].name = db.Name
		} else if sc := rev.Desc.GetSchema(); sc != nil {
			names[i].name = sc.Name
			names[i].parent = sc.ParentID
		} else if tb := rev.Desc.GetTable(); tb != nil {
			names[i].name = tb.Name
			names[i].parent = tb.ParentID
			names[i].parentSchema = keys.PublicSchemaID
			if s := tb.UnexposedParentSchemaID; s != descpb.InvalidID {
				names[i].parentSchema = s
			}
			if tb.Dropped() {
				names[i].id = 0
			}
		} else if typ := rev.Desc.GetType(); typ != nil {
			names[i].name = typ.Name
			names[i].parent = typ.ParentID
			names[i].parentSchema = typ.ParentSchemaID
		}
	}
	sort.Sort(names)

	for i, rev := range names {
		if i > 0 {
			prev := names[i-1]
			prev.ts = rev.ts
			if prev == rev {
				continue
			}
		}
		k := encodeNameSSTKey(rev.parent, rev.parentSchema, rev.name)
		v := encoding.EncodeUvarintAscending(nil, uint64(rev.id))
		if err := sst.PutMVCC(storage.MVCCKey{Key: k, Timestamp: rev.ts}, v); err != nil {
			return err
		}
	}

	return nil
}

func writeSpansToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
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
			if err := sst.PutMVCC(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeStatsToMetadata(ctx context.Context, sst storage.SSTWriter, stats []*stats.TableStatisticProto) error {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].TableID < stats[j].TableID || (stats[i].TableID == stats[j].TableID && stats[i].StatisticID < stats[j].StatisticID)
	})

	for _, i := range stats {
		b, err := protoutil.Marshal(i)
		if err != nil {
			return err
		}
		if err := sst.PutUnversioned(encodeStatSSTKey(i.TableID, i.StatisticID), b); err != nil {
			return err
		}
	}
	return nil
}

func writeTenantsToMetadata(ctx context.Context, sst storage.SSTWriter, m *BackupManifest) error {
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
	buf := encoding.EncodeBytesAscending([]byte(sstFilesPrefix), spanStart)
	return roachpb.Key(encoding.EncodeStringAscending(buf, filename))
}

func decodeUnsafeFileSSTKey(key roachpb.Key) (roachpb.Key, string, error) {
	key, err := deprefix(key, sstFilesPrefix)
	if err != nil {
		return nil, "", err
	}
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
	j, err := protoreflect.MessageToJSON(msg, false)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// DebugDumpMetadataSST is for debugging a metadata SST.
func DebugDumpMetadataSST(
	ctx context.Context,
	store cloud.ExternalStorage,
	path string,
	enc *jobspb.BackupEncryptionOptions,
	out func(rawKey, readableKey string, value json.JSON) error,
) error {
	var encOpts *roachpb.FileEncryptionOptions
	if enc != nil {
		key, err := getEncryptionKey(ctx, enc, store.Settings(), store.ExternalIOConf())
		if err != nil {
			return err
		}
		encOpts = &roachpb.FileEncryptionOptions{Key: key}
	}

	iter, err := storageccl.ExternalSSTReader(ctx, store, path, encOpts)
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
			info, err := pbBytesToJSON(iter.UnsafeValue(), &BackupManifest{})
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
			if v := iter.UnsafeValue(); len(v) > 0 {
				desc, err = pbBytesToJSON(v, &descpb.Descriptor{})
				if err != nil {
					return err
				}
			}
			if err := out(k.String(), fmt.Sprintf("desc %d @ %v", id, k.Timestamp), desc); err != nil {
				return err
			}

		case bytes.HasPrefix(k.Key, []byte(sstFilesPrefix)):
			spanStart, path, err := decodeUnsafeFileSSTKey(k.Key)
			if err != nil {
				return err
			}
			f, err := pbBytesToJSON(iter.UnsafeValue(), &BackupManifest_File{})
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("file %s (%s)", path, spanStart.String()), f); err != nil {
				return err
			}
		case bytes.HasPrefix(k.Key, []byte(sstNamesPrefix)):
			db, sc, name, err := decodeUnsafeNameSSTKey(k.Key)
			if err != nil {
				return err
			}
			var id uint64
			if v := iter.UnsafeValue(); len(v) > 0 {
				_, id, err = encoding.DecodeUvarintAscending(v)
				if err != nil {
					return err
				}
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
			s, err := pbBytesToJSON(iter.UnsafeValue(), &stats.TableStatisticProto{})
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
			i, err := pbBytesToJSON(iter.UnsafeValue(), &descpb.TenantInfo{})
			if err != nil {
				return err
			}
			if err := out(k.String(), fmt.Sprintf("tenant %d", id), i); err != nil {
				return err
			}

		default:
			if err := out(k.String(), "unknown", json.FromString(fmt.Sprintf("%q", iter.UnsafeValue()))); err != nil {
				return err
			}
		}
	}

	return nil
}
