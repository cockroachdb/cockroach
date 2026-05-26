// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// WriteCoverage writes one coverage epoch's manifest at
// log/coverage/<effective-from-HLC>. EffectiveFrom must match the
// path-name HLC (callers that construct one from the other
// naturally satisfy this).
func WriteCoverage(ctx context.Context, es cloud.ExternalStorage, c revlogpb.Coverage) error {
	if c.EffectiveFrom.IsEmpty() {
		return errors.AssertionFailedf("revlog: Coverage.EffectiveFrom must be set")
	}
	body, err := protoutil.Marshal(&c)
	if err != nil {
		return errors.Wrap(err, "marshaling coverage")
	}
	name := CoveragePath(c.EffectiveFrom)
	wc, err := es.Writer(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "opening coverage %s", name)
	}
	if _, err := wc.Write(EncodeFramed(body)); err != nil {
		_ = wc.Close()
		return errors.Wrap(err, "writing coverage")
	}
	return wc.Close()
}

// ReadCoverage downloads, verifies, and decodes one coverage
// epoch object at the given external-storage path.
func ReadCoverage(
	ctx context.Context, es cloud.ExternalStorage, name string,
) (revlogpb.Coverage, error) {
	rc, sz, err := es.ReadFile(ctx, name, cloud.ReadOptions{})
	if err != nil {
		return revlogpb.Coverage{}, errors.Wrapf(err, "reading %s", name)
	}
	defer func() { _ = rc.Close(ctx) }()
	buf, err := ioctx.ReadAllWithScratch(ctx, rc, make([]byte, 0, sz))
	if err != nil {
		return revlogpb.Coverage{}, errors.Wrapf(err, "reading %s", name)
	}
	body, err := DecodeFramed(buf)
	if err != nil {
		return revlogpb.Coverage{}, errors.Wrapf(err, "coverage %s", name)
	}
	var c revlogpb.Coverage
	if err := protoutil.Unmarshal(body, &c); err != nil {
		return revlogpb.Coverage{}, errors.Wrapf(err, "decoding coverage %s", name)
	}
	return c, nil
}

// CoverageAt returns the coverage in effect at asOf — the entry
// with the largest effective_from <= asOf — by LISTing
// log/coverage/ flat, sorting lex (= chronological per
// FormatHLCName), and binary-searching. Returns (zero, false, nil)
// if no entry has HLC <= asOf (i.e. asOf precedes the log's
// first coverage epoch).
//
// For long-lived logs, callers may want to cache the LIST result
// across queries; this helper re-LISTs every call.
func CoverageAt(
	ctx context.Context, es cloud.ExternalStorage, asOf hlc.Timestamp,
) (revlogpb.Coverage, bool, error) {
	var names []string
	err := es.List(ctx, CoverageRoot, cloud.ListOptions{}, func(name string) error {
		names = append(names, name)
		return nil
	})
	if err != nil {
		if isNotFound(err) {
			return revlogpb.Coverage{}, false, nil
		}
		return revlogpb.Coverage{}, false, errors.Wrapf(err, "listing %s", CoverageRoot)
	}
	if len(names) == 0 {
		return revlogpb.Coverage{}, false, nil
	}
	slices.Sort(names) // lex == chronological per FormatHLCName

	// Find the largest name whose parsed HLC is <= asOf. We walk
	// from the back since the typical asOf is recent.
	target := FormatHLCName(asOf)
	for i := len(names) - 1; i >= 0; i-- {
		// Different backends return List entries with different
		// conventions: see the matching normalization in reader.go's
		// Ticks. Strip a leading slash and the LIST prefix if
		// present.
		rel := strings.TrimPrefix(names[i], "/")
		rel = strings.TrimPrefix(rel, CoverageRoot)
		if rel > target {
			continue
		}
		c, err := ReadCoverage(ctx, es, CoverageRoot+rel)
		if err != nil {
			return revlogpb.Coverage{}, false, err
		}
		return c, true, nil
	}
	return revlogpb.Coverage{}, false, nil
}
