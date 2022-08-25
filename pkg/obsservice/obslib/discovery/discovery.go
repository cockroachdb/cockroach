// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package discovery

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

type targetDiscovery interface {
	List(ctx context.Context) ([]Target, error)
}

var _ targetDiscovery = &CRDBTargetDiscovery{}

type CRDBTargetDiscovery struct {
	db *pgxpool.Pool
}

func NewCRDBTargetDiscovery(pool *pgxpool.Pool) *CRDBTargetDiscovery {
	return &CRDBTargetDiscovery{
		db: pool,
	}
}

// List implements TargetDiscovery
func (d *CRDBTargetDiscovery) List(ctx context.Context) ([]Target, error) {
	rows, err := d.db.Query(ctx, "SELECT node_id, address, is_live FROM crdb_internal.gossip_nodes")
	if err != nil {
		panic(err)
	}
	var targets []Target
	for rows.Next() {
		v, _ := rows.Values()
		targets = append(targets, Target{
			nodeID:  roachpb.NodeID(v[0].(int64)),
			address: v[1].(string),
			active:  v[2].(bool),
		})
	}
	return targets, nil
}

type Target struct {
	nodeID  roachpb.NodeID
	address string
	active  bool
}

// PGURLString returns the string representation of a Target's PGURL
func (t Target) PGURLString() string {
	split := strings.Split(t.address, ":")
	host, port := split[0], split[1]
	return pgurl.New().
		WithTransport(pgurl.TransportNone()).
		WithDefaultUsername("root").
		WithDefaultPort(port).
		WithDefaultHost(host).
		String()
}
