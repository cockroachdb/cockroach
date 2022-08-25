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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/jackc/pgx/v4"
	"net"
)

// GossipTargetDiscovery is used to discover CRDB targets using the
// crdb_internal.gossip_nodes table.
type GossipTargetDiscovery struct {
	config Config
	db     *pgx.Conn
}

// MakeGossipTargetDiscovery returns a GossipTargetDiscovery object.
func MakeGossipTargetDiscovery(db *pgx.Conn, config Config) GossipTargetDiscovery {
	return GossipTargetDiscovery{
		config: config,
		db:     db,
	}
}

// list uses the crdb_internal.gossip_nodes table to return a list of known targets.
func (d *GossipTargetDiscovery) list(ctx context.Context) ([]Target, error) {
	rows, err := d.db.Query(
		ctx,
		"SELECT node_id, sql_address, address, is_live FROM crdb_internal.gossip_nodes",
	)
	if err != nil {
		return nil, err
	}
	var targets []Target
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			panic(err)
		}
		pgAddr, err := pgUrl(v[1].(string), d.config.TransportOpt)
		if err != nil {
			return nil, err
		}
		targets = append(targets, Target{
			NodeID:  roachpb.NodeID(v[0].(int64)),
			PGUrl:   pgAddr,
			RpcAddr: v[2].(string),
			Active:  v[3].(bool),
		})
	}
	rows.Close()
	return targets, rows.Err()
}

// Target is the internal representation of an obsservice monitoring target.
type Target struct {
	NodeID  roachpb.NodeID
	PGUrl   string
	RpcAddr string
	Active  bool
}

// Config is
type Config struct {
	InitPGUrl    string
	TransportOpt pgurl.TransportOption
}

// pgUrl converts the pgadress from crdb (host:port) into a pgurl with transport options.
func pgUrl(pgAddress string, opt pgurl.TransportOption) (string, error) {
	host, port, err := net.SplitHostPort(pgAddress)
	if err != nil {
		return "", err
	}
	return pgurl.New().
		WithTransport(opt).
		WithDefaultUsername(username.RootUser).
		WithDefaultPort(port).
		WithDefaultHost(host).
		String(), nil
}
