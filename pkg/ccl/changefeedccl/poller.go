// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type poller struct {
	g ctxgroup.Group

	changedKVsCh chan changedKVs
	doneCh       chan struct{}
}

func makePoller(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	spans []roachpb.Span,
	startTime, endTime hlc.Timestamp,
) (*poller, error) {
	p := &poller{
		changedKVsCh: make(chan changedKVs),
		doneCh:       make(chan struct{}, 1),
	}

	var ranges []roachpb.RangeDescriptor
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "fetching range descriptors")
	}

	type spanMarker struct{}
	type rangeMarker struct{}

	var spanCovering intervalccl.Covering
	for _, span := range spans {
		spanCovering = append(spanCovering, intervalccl.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering intervalccl.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, intervalccl.Range{
			Start:   []byte(rangeDesc.StartKey),
			End:     []byte(rangeDesc.EndKey),
			Payload: rangeMarker{},
		})
	}

	chunks := intervalccl.OverlapCoveringMerge(
		[]intervalccl.Covering{spanCovering, rangeCovering},
	)

	maxConcurrentExports := clusterNodeCount(execCfg.Gossip) *
		int(storage.ExportRequestsLimit.Get(&execCfg.Settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)

	p.g = ctxgroup.WithContext(ctx)
	p.g.GoCtx(func(ctx context.Context) error {
		sender := execCfg.DB.NonTransactionalSender()

		reqG := ctxgroup.WithContext(ctx)
		for _, chunk := range chunks {
			span := roachpb.Span{Key: chunk.Start, EndKey: chunk.End}
			if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case exportsSem <- struct{}{}:
			}

			reqG.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				if log.V(2) {
					log.Infof(ctx, `sending ExportRequest [%s,%s)`, span.Key, span.EndKey)
				}
				header := roachpb.Header{Timestamp: endTime}
				req := &roachpb.ExportRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(span),
					StartTime:     startTime,
					MVCCFilter:    roachpb.MVCCFilter_Latest,
					ReturnSST:     true,
				}
				res, pErr := client.SendWrappedWith(ctx, sender, header, req)
				if log.V(2) {
					log.Infof(ctx, `finished ExportRequest [%s,%s)`, span.Key, span.EndKey)
				}
				if pErr != nil {
					return errors.Wrapf(
						pErr.GoError(), `fetching changes for [%s,%s)`, span.Key, span.EndKey)
				}
				for _, file := range res.(*roachpb.ExportResponse).Files {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case p.changedKVsCh <- changedKVs{sst: file.SST}:
					}
				}
				return nil
			})
		}
		if err := reqG.Wait(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.changedKVsCh <- changedKVs{resolved: endTime}:
		}
		p.doneCh <- struct{}{}
		return nil
	})

	return p, nil
}

func (p *poller) get(ctx context.Context) (changedKVs, bool, error) {
	if p == nil {
		return changedKVs{}, false, nil
	}
	select {
	case <-ctx.Done():
		return changedKVs{}, false, ctx.Err()
	case <-p.doneCh:
		err := p.g.Wait()
		return changedKVs{}, false, err
	case c := <-p.changedKVsCh:
		return c, true, nil
	}
}

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	for k := range g.GetInfoStatus().Infos {
		if gossip.IsNodeIDKey(k) {
			nodes++
		}
	}
	return nodes
}
