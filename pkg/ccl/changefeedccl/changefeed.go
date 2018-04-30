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
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

var kafkaBootstrapServers = settings.RegisterStringSetting(
	"external.kafka.bootstrap_servers",
	"comma-separated list of host:port addresses of Kafka brokers to bootstrap the connection",
	"",
)
var changefeedPollInterval = settings.RegisterDurationSetting(
	"changefeed.experimental_poll_interval",
	"polling interval for the prototype changefeed implementation",
	1*time.Second,
)

func init() {
	changefeedPollInterval.Hide()
	sql.AddPlanHook(changefeedPlanHook)
	jobs.AddResumeHook(changefeedResumeHook)
}

const (
	changefeedOptTopicPrefix = "topic_prefix"
)

var changefeedOptionExpectValues = map[string]bool{
	changefeedOptTopicPrefix: true,
}

// changefeedPlanHook implements sql.PlanHookFn.
func changefeedPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	changefeedStmt, ok := stmt.(*tree.CreateChangefeed)
	if !ok {
		return nil, nil, nil, nil
	}

	switch strings.ToLower(changefeedStmt.SinkType) {
	case `kafka`:
	default:
		return nil, nil, nil, errors.Errorf(`unknown CHANGEFEED sink: %s`, changefeedStmt.SinkType)
	}

	optsFn, err := p.TypeAsStringOpts(changefeedStmt.Options, changefeedOptionExpectValues)
	if err != nil {
		return nil, nil, nil, err
	}

	header := sqlbase.ResultColumns{
		{Name: "job_id", Typ: types.Int},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		opts, err := optsFn()
		if err != nil {
			return err
		}

		now := p.ExecCfg().Clock.Now()
		var highwater hlc.Timestamp
		if changefeedStmt.AsOf.Expr != nil {
			var err error
			if highwater, err = sql.EvalAsOfTimestamp(nil, changefeedStmt.AsOf, now); err != nil {
				return err
			}
		}

		// TODO(dan): This grabs table descriptors once, but uses them to
		// interpret kvs written later. This both doesn't handle any schema
		// changes and breaks the table leasing.
		descriptorTime := now
		if highwater != (hlc.Timestamp{}) {
			descriptorTime = highwater
		}
		targetDescs, _, err := backupccl.ResolveTargetsToDescriptors(
			ctx, p, descriptorTime, changefeedStmt.Targets)
		if err != nil {
			return err
		}
		var tableDescs []sqlbase.TableDescriptor
		for _, desc := range targetDescs {
			if tableDesc := desc.GetTable(); tableDesc != nil {
				tableDescs = append(tableDescs, *tableDesc)
			}
		}

		job, _, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: changefeedJobDescription(changefeedStmt),
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, desc := range targetDescs {
					sqlDescIDs = append(sqlDescIDs, desc.GetID())
				}
				return sqlDescIDs
			}(),
			Details: jobs.ChangefeedDetails{
				Highwater:             highwater,
				TableDescs:            tableDescs,
				KafkaTopicPrefix:      opts[changefeedOptTopicPrefix],
				KafkaBootstrapServers: kafkaBootstrapServers.Get(&p.ExecCfg().Settings.SV),
			},
		})
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
		}
		return nil
	}
	return fn, header, nil, nil
}

func changefeedJobDescription(changefeed *tree.CreateChangefeed) string {
	return tree.AsStringWithFlags(changefeed, tree.FmtAlwaysQualifyTableNames)
}

type changefeed struct {
	jobID            int64
	kafkaTopicPrefix string
	spans            []roachpb.Span
	rf               sqlbase.RowFetcher

	db    *client.DB
	kafka sarama.SyncProducer

	a sqlbase.DatumAlloc
}

func newChangefeed(
	jobID int64,
	db *client.DB,
	kafkaTopicPrefix string,
	kafka sarama.SyncProducer,
	tableDescs []sqlbase.TableDescriptor,
) (*changefeed, error) {
	cf := &changefeed{
		jobID:            jobID,
		db:               db,
		kafkaTopicPrefix: kafkaTopicPrefix,
		kafka:            kafka,
	}

	var rfTables []sqlbase.RowFetcherTableArgs
	for _, tableDesc := range tableDescs {
		tableDesc := tableDesc
		if len(tableDesc.Families) != 1 {
			return nil, errors.Errorf(
				`only tables with 1 column family are currently supported: %s has %d`,
				tableDesc.Name, len(tableDesc.Families))
		}
		span := tableDesc.PrimaryIndexSpan()
		cf.spans = append(cf.spans, span)
		colIdxMap := make(map[sqlbase.ColumnID]int)
		var valNeededForCol util.FastIntSet
		// TODO(dan): Consider adding an option to only return primary key
		// columns. For some applications, this would be sufficient and (once we
		// support tables with more than one column family) would let us skip a
		// second round of requests .
		for colIdx, col := range tableDesc.Columns {
			colIdxMap[col.ID] = colIdx
			valNeededForCol.Add(colIdx)
		}
		rfTables = append(rfTables, sqlbase.RowFetcherTableArgs{
			Spans:            roachpb.Spans{span},
			Desc:             &tableDesc,
			Index:            &tableDesc.PrimaryIndex,
			ColIdxMap:        colIdxMap,
			IsSecondaryIndex: false,
			Cols:             tableDesc.Columns,
			ValNeededForCol:  valNeededForCol,
		})
	}
	if err := cf.rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &cf.a, rfTables...,
	); err != nil {
		return nil, err
	}

	// TODO(dan): Collapse any overlapping cf.spans (which only happens for
	// interleaved tables).

	return cf, nil
}

func (cf *changefeed) Close() error {
	return cf.kafka.Close()
}

func (cf *changefeed) poll(ctx context.Context, startTime, endTime hlc.Timestamp) error {
	log.VEventf(ctx, 1, `changefeed poll job %d [%s,%s)`, cf.jobID, startTime, endTime)

	// TODO(dan): Write a KVFetcher implementation backed by a sequence of
	// sstables.
	var kvs sqlbase.SpanKVFetcher
	emitFunc := func(kv engine.MVCCKeyValue) (bool, error) {
		if log.V(3) {
			v := roachpb.Value{RawBytes: kv.Value}
			log.Infof(ctx, `kv %s [%s] -> %s`, kv.Key.Key, kv.Key.Timestamp, v.PrettyPrint())
		}
		// TODO(dan): Plumb this timestamp down to record written to kafka.
		kvs.KVs = append(kvs.KVs, roachpb.KeyValue{
			Key: kv.Key.Key,
			Value: roachpb.Value{
				Timestamp: kv.Key.Timestamp,
				RawBytes:  kv.Value,
			},
		})
		return false, nil
	}

	// TODO(dan): Send these out in parallel.
	for _, span := range cf.spans {
		header := roachpb.Header{Timestamp: endTime}
		req := &roachpb.ExportRequest{
			Span:       roachpb.Span{Key: span.Key, EndKey: span.EndKey},
			StartTime:  startTime,
			MVCCFilter: roachpb.MVCCFilter_Latest,
			ReturnSST:  true,
		}
		res, pErr := client.SendWrappedWith(ctx, cf.db.GetSender(), header, req)
		if pErr != nil {
			return errors.Wrapf(
				pErr.GoError(), `fetching changes for [%s,%s)`, span.Key, span.EndKey)
		}
		for _, file := range res.(*roachpb.ExportResponse).Files {
			err := func() error {
				sst := engine.MakeRocksDBSstFileReader()
				defer sst.Close()
				if err := sst.IngestExternalFile(file.SST); err != nil {
					return err
				}
				start, end := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}
				return sst.Iterate(start, end, emitFunc)
			}()
			if err != nil {
				return err
			}
		}
	}

	if err := cf.rf.StartScanFrom(ctx, &kvs); err != nil {
		return err
	}

	for {
		// TODO(dan): Handle DELETEs. This uses RowFetcher out of convenience
		// (specifically for kv decoding and interleaved tables), but it's not
		// built to output deletes.
		row, tableDesc, _, err := cf.rf.NextRowDecoded(ctx)
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		jsonEntries := make(map[string]interface{}, len(row))
		for i := range row {
			jsonEntries[tableDesc.Columns[i].Name], err = builtins.AsJSON(row[i])
			if err != nil {
				return err
			}
		}
		j, err := json.MakeJSON(jsonEntries)
		if err != nil {
			return err
		}
		json := j.String()
		if log.V(3) {
			log.Infof(ctx, `row %s`, json)
		}

		message := &sarama.ProducerMessage{
			Topic: cf.kafkaTopicPrefix + tableDesc.Name,
			Value: sarama.ByteEncoder(json),
		}
		if _, _, err := cf.kafka.SendMessage(message); err != nil {
			return errors.Wrapf(err, `sending message to kafka topic %s`, message.Topic)
		}
	}

	return nil
}

type changefeedResumer struct {
	settings *cluster.Settings
}

func (b *changefeedResumer) Resume(
	ctx context.Context, job *jobs.Job, planHookState interface{}, _ chan<- tree.Datums,
) error {
	details := job.Record.Details.(jobs.ChangefeedDetails)
	p := planHookState.(sql.PlanHookState)

	producer, err := getKafkaProducer(details.KafkaBootstrapServers)
	if err != nil {
		return err
	}

	cf, err := newChangefeed(
		*job.ID(), p.ExecCfg().DB, details.KafkaTopicPrefix, producer, details.TableDescs)
	if err != nil {
		return err
	}
	defer func() { _ = cf.Close() }()

	highwater := details.Highwater
	for {
		nextHighwater := p.ExecCfg().Clock.Now()
		// TODO(dan): nextHighwater should probably be some amount of time in
		// the past, so we don't update a bunch of timestamp caches and cause
		// transactions to be restarted.
		if err := cf.poll(ctx, highwater, nextHighwater); err != nil {
			return err
		}
		highwater = nextHighwater
		log.VEventf(ctx, 1, `new highwater: %s`, highwater)

		// TODO(dan): HACK for testing. We call SendMessages with nil to
		// indicate to the test that a full poll finished. Figure out something
		// better.
		if err := producer.SendMessages(nil); err != nil {
			return err
		}

		progressedFn := func(ctx context.Context, details jobs.Details) float32 {
			cfDetails := details.(*jobs.Payload_Changefeed).Changefeed
			cfDetails.Highwater = nextHighwater
			// TODO(dan): Having this stuck at 0% forever is bad UX. Revisit.
			return 0.0
		}
		if err := job.Progressed(ctx, progressedFn); err != nil {
			return err
		}

		pollDuration := changefeedPollInterval.Get(&p.ExecCfg().Settings.SV)
		pollDuration = pollDuration - timeutil.Since(timeutil.Unix(0, highwater.WallTime))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollDuration): // NB: time.After handles durations < 0
		}
	}
}

func (b *changefeedResumer) OnFailOrCancel(context.Context, *client.Txn, *jobs.Job) error { return nil }
func (b *changefeedResumer) OnSuccess(context.Context, *client.Txn, *jobs.Job) error      { return nil }
func (b *changefeedResumer) OnTerminal(
	context.Context, *jobs.Job, jobs.Status, chan<- tree.Datums,
) {
}

func changefeedResumeHook(typ jobs.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobs.TypeChangefeed {
		return nil
	}
	return &changefeedResumer{settings: settings}
}
