// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func makeTimestamps(n int) []hlc.Timestamp {
	timestamps := make([]hlc.Timestamp, n)
	for i := range timestamps {
		timestamps[i] = hlc.Timestamp{WallTime: int64(i * 10)}
	}

	return timestamps
}

// MakeImportSpans looks for the following properties:
//  - Start time
//  - End time
//  - Spans
//  - Introduced spans
//  - Files
func makeBackupManifest(
	startTime, endTime hlc.Timestamp, spans, introducedSpans []roachpb.Span,
) BackupManifest {
	// We only care about the files' span.
	files := make([]BackupManifest_File, 0)
	for i, span := range append(spans, introducedSpans...) {
		files = append(files, BackupManifest_File{Span: span, Path: fmt.Sprintf("data/%d.sst", i)})
	}

	return BackupManifest{
		StartTime:       startTime,
		EndTime:         endTime,
		Spans:           spans,
		IntroducedSpans: introducedSpans,
		Files:           files,
	}
}

func makeTableSpan(tableID uint32) roachpb.Span {
	k := keys.SystemSQLCodec.TablePrefix(tableID)
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

func TestMakeImportSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := makeTimestamps(10)

	var backupLocalityMap map[int]storeByLocalityKV
	lowWaterMark := roachpb.KeyMin

	noIntroducedSpans := make([]roachpb.Span, 0)
	onMissing := errOnMissingRange

	tcs := []struct {
		name            string
		tablesToRestore []roachpb.Span
		backups         []BackupManifest

		// In the successful cases, expectedSpans and endTime should be
		// specified.
		expectedSpans      []roachpb.Span
		expectedMaxEndTime hlc.Timestamp

		// In the error case, only the error is checked.
		expectedError string
	}{
		{
			name:            "single-backup",
			tablesToRestore: []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			expectedMaxEndTime: ts[1],
		},
		{
			name:            "incremental-backup",
			tablesToRestore: []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
				// Now add an incremental backup of the same tables.
				makeBackupManifest(
					ts[1], ts[2],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			expectedMaxEndTime: ts[2],
		},
		{
			name: "restore-subset",
			// Restore only a sub-set of the spans that have been backed up.
			tablesToRestore: []roachpb.Span{makeTableSpan(52)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[1], ts[2],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52)},
			expectedMaxEndTime: ts[2],
		},
		{
			// Try backing up a non-new table in an incremental backup.
			name:            "widen-backup",
			tablesToRestore: []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			backups: []BackupManifest{
				// The full backup only has table 52.
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52)},
					noIntroducedSpans,
				),
				// This incremental claims to have backed up more.
				makeBackupManifest(
					ts[1], ts[2],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedError: "no backup covers time [0,0,0.000000010,0) for range [/Table/53,/Table/54) or backups listed out of order (mismatched start time)",
		},
		{
			name:            "narrow-backup",
			tablesToRestore: []roachpb.Span{makeTableSpan(52)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					// This full backup backs up both tables 52 and 53.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[1], ts[2],
					// This incremental decided to only backup table 52. That's ok.
					[]roachpb.Span{makeTableSpan(52)},
					noIntroducedSpans,
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52)},
			expectedMaxEndTime: ts[2],
		},
		{
			name:            "narrow-backup-rewident",
			tablesToRestore: []roachpb.Span{makeTableSpan(52)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					// This full backup backs up both tables 52 and 53.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[1], ts[2],
					// This incremental decided to only backup table 52. That's
					// permitted.
					[]roachpb.Span{makeTableSpan(52)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[2], ts[3],
					// We can't start backing up table 53 again after an
					// incremental missed it though.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedError: "no backup covers time [0.000000010,0,0.000000020,0) for range [/Table/53,/Table/54) or backups listed out of order (mismatched start time)",
		},
		{
			name:            "incremental-newly-created-table",
			tablesToRestore: []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[1], ts[2],
					// We're now backing up a new table (53), but this is only
					// allowed since this table didn't exist at the time of the
					// full backup. It must appear in introduced spans.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					// Table 53 was created between the full backup and this
					// inc, so it appears as introduced spans.
					[]roachpb.Span{makeTableSpan(53)}, // introduced spans
				),
				makeBackupManifest(
					ts[2], ts[3],
					// We should be able to backup table 53 incremenatally after
					// it has been introduced.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			expectedMaxEndTime: ts[3],
		},
		{
			name:            "reintroduced-spans",
			tablesToRestore: []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			backups: []BackupManifest{
				makeBackupManifest(
					ts[0], ts[1],
					[]roachpb.Span{makeTableSpan(52)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[1], ts[2],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					// Table 53 was created between the full backup and this
					// inc, so it appears as introduced spans.
					[]roachpb.Span{makeTableSpan(53)}, // introduced spans
				),
				makeBackupManifest(
					ts[2], ts[3],
					// We should be able to backup table 53 incremenatally after
					// it has been introduced.
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					noIntroducedSpans,
				),
				makeBackupManifest(
					ts[3], ts[4],
					[]roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
					// In some cases, spans that were normally included in
					// incremental backups may be re-introduced.
					[]roachpb.Span{makeTableSpan(53)},
				),
			},

			expectedSpans:      []roachpb.Span{makeTableSpan(52), makeTableSpan(53)},
			expectedMaxEndTime: ts[4],
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			importSpans, maxEndTime, err := makeImportSpans(
				tc.tablesToRestore, tc.backups, backupLocalityMap,
				lowWaterMark, onMissing)

			// Collect just the spans to import.
			spansToImport := make([]roachpb.Span, len(importSpans))
			for i, importSpan := range importSpans {
				spansToImport[i] = importSpan.Span
			}

			if len(tc.expectedError) != 0 {
				require.Equal(t, tc.expectedError, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedSpans, spansToImport)
				require.Equal(t, tc.expectedMaxEndTime, maxEndTime)
			}
		})
	}
}
