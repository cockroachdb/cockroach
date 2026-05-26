// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

func NewCdcEventDecoder(
	ctx context.Context, tables []TableMapping, settings *cluster.Settings,
) (cdcevent.Decoder, error) {
	cdcEventTargets := changefeedbase.Targets{}
	srcTablesBySrcID := make(map[descpb.ID]catalog.TableDescriptor, len(tables))

	for _, table := range tables {
		descriptor := table.SourceDescriptor
		srcTablesBySrcID[descriptor.GetID()] = descriptor
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			DescID:            descriptor.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(descriptor.GetName()),
		})
	}

	prefixlessCodec := keys.SystemSQLCodec
	rfCache, err := cdcevent.NewFixedRowFetcherCache(
		ctx, prefixlessCodec, settings, cdcEventTargets, srcTablesBySrcID,
	)
	if err != nil {
		return nil, err
	}

	return cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false, cdcevent.DecoderOptions{}), nil
}
