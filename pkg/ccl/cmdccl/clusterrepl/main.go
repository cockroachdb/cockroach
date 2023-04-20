// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx/v4"
)

var (
	uri    = flag.String("uri", "", "sql uri")
	tenant = flag.String("tenant", "", "tenant name")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	config, err := pgx.ParseConfig(*uri)
	if err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}

	fmt.Printf("start stream\n")
	var rawReplicationProducerSpec []byte
	row := conn.QueryRow(ctx, `SELECT crdb_internal.start_replication_stream($1)`, *tenant)
	if err := row.Scan(&rawReplicationProducerSpec); err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}
	var replicationProducerSpec streampb.ReplicationProducerSpec
	if err := protoutil.Unmarshal(rawReplicationProducerSpec, &replicationProducerSpec); err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}

	tenantID := getTenantID(ctx, conn, *tenant)
	prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID))
	span := &roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}

	var sps streampb.StreamPartitionSpec
	sps.Spans = append(sps.Spans, *span)
	sps.InitialScanTimestamp = replicationProducerSpec.ReplicationStartTime

	spsBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}

	_, err = conn.Exec(ctx, `SET avoid_buffering = true`)
	if err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}

	fmt.Printf("stream partition\n")
	rows, err := conn.Query(ctx, `SELECT * FROM crdb_internal.stream_partition($1, $2)`,
		replicationProducerSpec.StreamID, spsBytes)
	if err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}
	defer rows.Close()

	getNextEvent := func() {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				log.Fatalf(ctx, "Error: %v", err)
			}
			return
		}
		var data []byte
		if err := rows.Scan(&data); err != nil {
			log.Fatalf(ctx, "Error: %v", err)
		}
		var streamEvent streampb.StreamEvent
		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			log.Fatalf(ctx, "Error: %v", err)
		}
		fmt.Printf("event size: %d\n", streamEvent.Size())
	}

	fmt.Printf("streaming..\n")

	for {
		getNextEvent()
	}
}

func getTenantID(ctx context.Context, conn *pgx.Conn, tenantName string) uint64 {
	var tenantID uint64
	row := conn.QueryRow(ctx, `SELECT id FROM [SHOW TENANT $1]`, tenantName)
	if err := row.Scan(&tenantID); err != nil {
		log.Fatalf(ctx, "Error: %v", err)
	}
	return tenantID
}
