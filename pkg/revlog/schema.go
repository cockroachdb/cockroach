// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"context"
	"iter"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// WriteSchemaDesc records one descriptor change at
// log/schema/descs/<changedAt>/<descID>.pb. desc is the
// descriptor's new state (any state, including DROP); pass nil to
// write a tombstone marking the descriptor's deletion from KV
// (a 4-byte payload-less object).
func WriteSchemaDesc(
	ctx context.Context,
	es cloud.ExternalStorage,
	changedAt hlc.Timestamp,
	descID descpb.ID,
	desc *descpb.Descriptor,
) error {
	if changedAt.IsEmpty() {
		return errors.AssertionFailedf("revlog: schema desc changedAt must be set")
	}
	var body []byte
	if desc != nil {
		var err error
		body, err = protoutil.Marshal(desc)
		if err != nil {
			return errors.Wrap(err, "marshaling descriptor")
		}
	}
	name := SchemaDescPath(changedAt, int64(descID))
	wc, err := es.Writer(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "opening schema desc %s", name)
	}
	if _, err := wc.Write(EncodeFramed(body)); err != nil {
		_ = wc.Close()
		return errors.Wrap(err, "writing schema desc")
	}
	return wc.Close()
}

// SchemaChange is one descriptor change yielded by
// IterSchemaChanges. Descriptor is nil iff the change is a
// tombstone (descriptor deleted from KV).
type SchemaChange struct {
	ChangedAt  hlc.Timestamp
	DescID     descpb.ID
	Descriptor *descpb.Descriptor
}

// ReadSchemaDesc downloads, verifies, and decodes one
// descriptor-change object at the given path. Returns
// (nil, nil) for a tombstone (zero-byte payload).
func ReadSchemaDesc(
	ctx context.Context, es cloud.ExternalStorage, name string,
) (*descpb.Descriptor, error) {
	rc, sz, err := es.ReadFile(ctx, name, cloud.ReadOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "reading %s", name)
	}
	defer func() { _ = rc.Close(ctx) }()
	buf, err := ioctx.ReadAllWithScratch(ctx, rc, make([]byte, 0, sz))
	if err != nil {
		return nil, errors.Wrapf(err, "reading %s", name)
	}
	body, err := DecodeFramed(buf)
	if err != nil {
		return nil, errors.Wrapf(err, "schema desc %s", name)
	}
	if len(body) == 0 {
		return nil, nil
	}
	desc := &descpb.Descriptor{}
	if err := protoutil.Unmarshal(body, desc); err != nil {
		return nil, errors.Wrapf(err, "decoding descriptor %s", name)
	}
	return desc, nil
}

// IterSchemaChanges iterates descriptor changes whose changedAt
// HLC falls in (start, end] — start exclusive, end inclusive,
// matching the tick read window's semantics. Yields entries in
// (changedAt, desc_id) ascending order. Tombstones (deleted
// descriptors) are yielded with Descriptor == nil.
//
// Discovery is a flat LIST under log/schema/descs/ (no prefix
// narrowing yet — schema changes are infrequent so a full LIST is
// cheap; if log lifetimes get long enough that this matters, the
// same LIST-prefix trick from Ticks could apply).
//
// Listing or read errors abort iteration and are yielded once.
func IterSchemaChanges(
	ctx context.Context, es cloud.ExternalStorage, start, end hlc.Timestamp,
) iter.Seq2[SchemaChange, error] {
	return func(yield func(SchemaChange, error) bool) {
		if !start.Less(end) {
			return
		}
		var names []string
		err := es.List(ctx, SchemaDescsRoot, cloud.ListOptions{}, func(name string) error {
			names = append(names, name)
			return nil
		})
		if err != nil {
			if isNotFound(err) {
				return
			}
			yield(SchemaChange{}, errors.Wrapf(err, "listing %s", SchemaDescsRoot))
			return
		}
		slices.Sort(names) // lex == (HLC asc, descID asc)

		startName := FormatHLCName(start)
		endName := FormatHLCName(end)
		for _, name := range names {
			rel := strings.TrimPrefix(name, "/")
			rel = strings.TrimPrefix(rel, SchemaDescsRoot)
			// rel = "<HLC-name>/<desc-id>.pb"
			slash := strings.IndexByte(rel, '/')
			if slash < 0 {
				continue
			}
			hlcName := rel[:slash]
			descSuffix := strings.TrimSuffix(rel[slash+1:], markerExt)
			if descSuffix == rel[slash+1:] {
				yield(SchemaChange{},
					errors.Errorf("unexpected schema-desc object %q (missing %q suffix)", name, markerExt))
				return
			}
			// Window check: hlcName in (startName, endName].
			if hlcName <= startName {
				continue
			}
			if hlcName > endName {
				return // sorted; nothing else can be in window
			}
			changedAt, err := ParseHLCName(hlcName)
			if err != nil {
				yield(SchemaChange{}, errors.Wrapf(err, "parsing HLC in %q", name))
				return
			}
			descID, err := strconv.ParseInt(descSuffix, 10, 64)
			if err != nil {
				yield(SchemaChange{}, errors.Wrapf(err, "parsing desc-id in %q", name))
				return
			}
			desc, err := ReadSchemaDesc(ctx, es, SchemaDescsRoot+rel)
			if err != nil {
				yield(SchemaChange{}, err)
				return
			}
			if !yield(SchemaChange{
				ChangedAt:  changedAt,
				DescID:     descpb.ID(descID),
				Descriptor: desc,
			}, nil) {
				return
			}
		}
	}
}
