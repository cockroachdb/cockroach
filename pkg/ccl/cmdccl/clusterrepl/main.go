// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

var (
	uri    = flag.String("uri", "", "sql uri")
	tenant = flag.String("tenant", "", "tenant name")

	batchSize          = flag.Int("batch-size", 1<<20, "batch size")
	checkpointInterval = flag.Duration("checkpoint-iterval", 10*time.Second, "checkpoint interval")
	statsInterval      = flag.Duration("stats-interval", 5*time.Second, "period over which to measure throughput")

	noParse = flag.Bool("no-parse", false, "avoid parsing data")
)

func errorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: %s\n", fmt.Sprintf(msg, args...))
}

func fatalf(msg string, args ...interface{}) {
	errorf(msg, args...)
	exit.WithCode(exit.UnspecifiedError())
}

func cancelOnShutdown(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		fmt.Println("received interrupt, shutting down; send another interrupt to force kill")
		cancel()
		<-c
		fatalf("received second interrupt, shutting down now\n")
	}()
	return ctx
}

func main() {
	flag.Parse()
	if *uri == "" {
		fatalf("URI required")
	}
	if *tenant == "" {
		fatalf("tenant name required")
	}

	uri, err := streamclient.ParseClusterUri(*uri)
	if err != nil {
		fatalf("parse: %s", err)
	}

	ctx := cancelOnShutdown(context.Background())
	if err := streamPartition(ctx, uri); err != nil {
		if errors.Is(err, context.Canceled) {
			exit.WithCode(exit.Interrupted())
		} else {
			fatalf("%s", err)
		}
	}
}

func streamPartition(ctx context.Context, uri streamclient.ClusterUri) error {
	fmt.Println("creating producer stream")
	client, err := streamclient.NewPartitionedStreamClient(ctx, uri)
	if err != nil {
		return err
	}

	replicationProducerSpec, err := client.CreateForTenant(ctx, roachpb.TenantName(*tenant), streampb.ReplicationProducerRequest{})
	if err != nil {
		return err
	}

	defer func() {
		// Attempt to clean up the replication job on shutdown. But don't wait forever.
		if err := timeutil.RunWithTimeout(ctx, "complete-stream", 30*time.Second, func(ctx context.Context) error {
			return client.Complete(ctx, replicationProducerSpec.StreamID, true)
		}); err != nil {
			errorf("warning: could not clean up producer job: %s", err.Error())
		}
	}()

	// We ignore most of this plan. But, it gives us the tenant ID.
	plan, err := client.PlanPhysicalReplication(ctx, replicationProducerSpec.StreamID)
	if err != nil {
		return err
	}

	tenantSpan := keys.MakeTenantSpan(plan.SourceTenantID)

	var sps streampb.StreamPartitionSpec
	sps.Config.MinCheckpointFrequency = *checkpointInterval
	sps.Config.BatchByteSize = int64(*batchSize)
	sps.Spans = append(sps.Spans, tenantSpan)
	sps.InitialScanTimestamp = replicationProducerSpec.ReplicationStartTime
	spsBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return err
	}
	frontier, err := span.MakeFrontier(sps.Spans...)
	if err != nil {
		return err
	}

	fmt.Printf("streaming %s (%s) as of %s\n", *tenant, tenantSpan, sps.InitialScanTimestamp)
	if *noParse {
		return rawStream(ctx, uri, replicationProducerSpec.StreamID, spsBytes)
	}

	sub, err := client.Subscribe(ctx, replicationProducerSpec.StreamID, 1, 1,
		spsBytes,
		sps.InitialScanTimestamp,
		nil)
	if err != nil {
		return err
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(sub.Subscribe)
	g.GoCtx(subscriptionConsumer(sub, frontier))
	return g.Wait()
}

func rawStream(
	ctx context.Context,
	uri streamclient.ClusterUri,
	streamID streampb.StreamID,
	spec streamclient.SubscriptionToken,
) error {
	config, err := pgx.ParseConfig(uri.Serialize())
	if err != nil {
		return err
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, `SET avoid_buffering = true`)
	if err != nil {
		return err
	}
	rows, err := conn.Query(ctx, `SELECT * FROM crdb_internal.stream_partition($1, $2)`,
		streamID, spec)
	if err != nil {
		return err
	}
	defer rows.Close()

	var data []byte
	var (
		totalBytes    int
		intervalBytes int

		totalRowCount    int
		intervalRowCount int
	)
	intervalStart := timeutil.Now()
	for {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return nil
		}
		data = data[:0]
		if err := rows.Scan(&data); err != nil {
			return err
		}
		totalBytes += len(data)
		intervalBytes += len(data)
		totalRowCount++
		intervalRowCount++

		if timeutil.Since(intervalStart) > *statsInterval {
			var (
				elapsedSeconds = timeutil.Since(intervalStart).Seconds()
				bps            = humanizeutil.IBytes(int64(float64(intervalBytes) / elapsedSeconds))
				total          = humanizeutil.IBytes(int64(totalBytes))
				averageRowSize = humanizeutil.IBytes(int64(intervalBytes / intervalRowCount))
			)
			fmt.Printf("%s throughput: %s/s; total transfered: %s; average row size: %s\n",
				timeutil.Now().Format(time.RFC3339),
				bps, total, averageRowSize)
			intervalStart = timeutil.Now()
			intervalBytes = 0
			intervalRowCount = 0
		}
	}
}

func subscriptionConsumer(
	sub streamclient.Subscription, frontier span.Frontier,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		var (
			totalBytes    int
			intervalBytes int

			totalEventCount    int
			intervalEventCount int
		)
		defer frontier.Release()

		intervalStart := timeutil.Now()
		for {
			var sz int
			select {
			case event, ok := <-sub.Events():
				if !ok {
					return sub.Err()
				}
				switch event.Type() {
				case crosscluster.KVEvent:
					sz = 0
					for _, kv := range event.GetKVs() {
						sz += kv.Size()
					}
				case crosscluster.SSTableEvent:
					ssTab := event.GetSSTable()
					sz = ssTab.Size()
				case crosscluster.DeleteRangeEvent:
				case crosscluster.CheckpointEvent:
					fmt.Printf("%s checkpoint\n", timeutil.Now().Format(time.RFC3339))
					for _, r := range event.GetCheckpoint().ResolvedSpans {
						_, err := frontier.Forward(r.Span, r.Timestamp)
						if err != nil {
							return err
						}
					}
				default:
					return errors.Newf("unknown streaming event type %v", event.Type())
				}
			case <-ctx.Done():
				return nil
			}
			totalBytes += sz
			intervalBytes += sz
			totalEventCount++
			intervalEventCount++

			if timeutil.Since(intervalStart) > *statsInterval {
				var (
					elapsedSeconds   = timeutil.Since(intervalStart).Seconds()
					bps              = humanizeutil.IBytes(int64(float64(intervalBytes) / elapsedSeconds))
					total            = humanizeutil.IBytes(int64(totalBytes))
					averageEventSize = humanizeutil.IBytes(int64(intervalBytes / intervalEventCount))
				)
				fmt.Printf("%s throughput: %s/s; total transfered: %s; average event size: %s; frontier: %s\n",
					timeutil.Now().Format(time.RFC3339),
					bps, total, averageEventSize, frontier.Frontier())
				intervalStart = timeutil.Now()
				intervalBytes = 0
				intervalEventCount = 0
			}
		}
	}
}
