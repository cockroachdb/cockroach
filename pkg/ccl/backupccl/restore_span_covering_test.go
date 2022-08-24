// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// MockBackupChain returns a chain of mock backup manifests that have spans and
// file spans suitable for checking coverage computations. Every 3rd inc backup
// introduces a span and drops a span. Incremental backups have half as many
// files as the base. Files spans are ordered by start key but may overlap.
func MockBackupChain(length, spans, baseFiles int, r *rand.Rand) []backuppb.BackupManifest {
	backups := make([]backuppb.BackupManifest, length)
	ts := hlc.Timestamp{WallTime: time.Second.Nanoseconds()}
	for i := range backups {
		backups[i].Spans = make(roachpb.Spans, spans)
		for j := range backups[i].Spans {
			backups[i].Spans[j] = makeTableSpan(uint32(100 + j + (i / 3)))
		}
		backups[i].EndTime = ts.Add(time.Minute.Nanoseconds()*int64(i), 0)
		if i > 0 {
			backups[i].StartTime = backups[i-1].EndTime
			if i%3 == 0 {
				backups[i].IntroducedSpans = roachpb.Spans{backups[i].Spans[spans-1]}
			}
		}

		files := baseFiles
		if i == 0 {
			backups[i].Files = make([]backuppb.BackupManifest_File, files)
		} else {
			files = baseFiles / 2
			backups[i].Files = make([]backuppb.BackupManifest_File, files)
		}

		for f := range backups[i].Files {
			start := f*5 + r.Intn(4)
			end := start + 1 + r.Intn(25)
			k := encoding.EncodeVarintAscending(backups[i].Spans[f*spans/files].Key, 1)
			k = k[:len(k):len(k)]
			backups[i].Files[f].Span.Key = encoding.EncodeVarintAscending(k, int64(start))
			backups[i].Files[f].Span.EndKey = encoding.EncodeVarintAscending(k, int64(end))
			backups[i].Files[f].Path = fmt.Sprintf("12345-b%d-f%d.sst", i, f)
		}
		// A non-nil Dir more accurately models the footprint of produced coverings.
		backups[i].Dir = cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{}}
	}
	return backups
}

// checkRestoreCovering verifies that a covering actually uses every span of
// every file in the passed backups that overlaps with any part of the passed
// spans. It does by constructing a map from every file name to a SpanGroup that
// contains the overlap of that file span with every required span, and then
// iterating through the partitions of the cover and removing that partition's
// span from the group for every file specified by that partition, and then
// checking that all the groups are empty, indicating no needed span was missed.
// It also checks that each file that the cover has an expected number of
// partitions (i.e. isn't just one big partition of all files), by comparing its
// length to the number of files a file's end key was greater than any prior end
// key when walking files in order by start key in the backups. This check is
// thus sensitive to ordering; the coverage correctness check however is not.
func checkRestoreCovering(
	backups []backuppb.BackupManifest, spans roachpb.Spans, cov []execinfrapb.RestoreSpanEntry,
) error {
	var expectedPartitions int
	required := make(map[string]*roachpb.SpanGroup)
	for _, s := range spans {
		var last roachpb.Key
		for _, b := range backups {
			for _, f := range b.Files {
				if sp := s.Intersect(f.Span); sp.Valid() {
					if required[f.Path] == nil {
						required[f.Path] = &roachpb.SpanGroup{}
					}
					required[f.Path].Add(sp)
					if sp.EndKey.Compare(last) > 0 {
						last = sp.EndKey
						expectedPartitions++
					}
				}
			}
		}
	}
	for _, c := range cov {
		for _, f := range c.Files {
			required[f.Path].Sub(c.Span)
		}
	}
	for name, uncovered := range required {
		for _, missing := range uncovered.Slice() {
			return errors.Errorf("file %s is was supposed to cover span %s", name, missing)
		}
	}
	if got := len(cov); got != expectedPartitions {
		return errors.Errorf("expected at least %d partitions, got %d", expectedPartitions, got)
	}
	return nil
}

func TestRestoreEntryCoverExample(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sp := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	f := func(start, end, path string) backuppb.BackupManifest_File {
		return backuppb.BackupManifest_File{Span: sp(start, end), Path: path}
	}
	paths := func(names ...string) []execinfrapb.RestoreFileSpec {
		r := make([]execinfrapb.RestoreFileSpec, len(names))
		for i := range names {
			r[i].Path = names[i]
		}
		return r
	}

	// Setup and test the example in the comnent on makeSimpleImportSpans.
	spans := []roachpb.Span{sp("a", "f"), sp("f", "i"), sp("l", "m")}
	backups := []backuppb.BackupManifest{
		{Files: []backuppb.BackupManifest_File{f("a", "c", "1"), f("c", "e", "2"), f("h", "i", "3")}},
		{Files: []backuppb.BackupManifest_File{f("b", "d", "4"), f("g", "i", "5")}},
		{Files: []backuppb.BackupManifest_File{f("a", "h", "6"), f("j", "k", "7")}},
		{Files: []backuppb.BackupManifest_File{f("h", "i", "8"), f("l", "m", "9")}},
	}
	cover := makeSimpleImportSpans(spans, backups, nil, nil)
	require.Equal(t, []execinfrapb.RestoreSpanEntry{
		{Span: sp("a", "c"), Files: paths("1", "4", "6")},
		{Span: sp("c", "e"), Files: paths("2", "4", "6")},
		{Span: sp("e", "f"), Files: paths("6")},
		{Span: sp("f", "i"), Files: paths("3", "5", "6", "8")},
		{Span: sp("l", "m"), Files: paths("9")},
	}, cover)
}

func TestRestoreEntryCover(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewTestRand()
	for _, numBackups := range []int{1, 2, 3, 5, 9, 10, 11, 12} {
		for _, spans := range []int{1, 2, 3, 5, 9, 11, 12} {
			for _, files := range []int{0, 1, 2, 3, 4, 10, 12, 50} {
				backups := MockBackupChain(numBackups, spans, files, r)

				t.Run(fmt.Sprintf("numBackups=%d, numSpans=%d, numFiles=%d", numBackups, spans, files), func(t *testing.T) {
					cover := makeSimpleImportSpans(backups[numBackups-1].Spans, backups, nil, nil)
					if err := checkRestoreCovering(backups, backups[numBackups-1].Spans, cover); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
