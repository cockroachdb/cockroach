package backupinfo

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSpanComparator(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	f := &storage.MemObject{}
	sst := storage.MakeBackupSSTWriter(context.Background(), st, &f.Buffer)
	defer sst.Close()

	// "span/\x12\xfe\x96\x00\x01\x12\xfe\x98\x00\x01"/0,0#0,SET
	// "span/\x12\xfe\x96\x00\x01\x12\xfe\x97\x00\x01"/1699264047.460111000,0#0,SET
	m := backuppb.BackupManifest {
		Spans: []roachpb.Span {{
			Key: roachpb.Key { 1 },
			EndKey: roachpb.Key { 2 },
		}},
		IntroducedSpans: []roachpb.Span {{
			Key: roachpb.Key { 1 },
			EndKey: roachpb.Key { 3 },
		}},
	}

	require.NoError(t, writeSpansToMetadata(context.Background(), sst, &m))
}
