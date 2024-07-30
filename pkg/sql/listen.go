// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) Listen(ctx context.Context, n *tree.Listen) (planNode, error) {
	// Run an EXPERIMENTAL CHANGEFEED plan/stmt basically with a different sort of sink that hooks up to this guy

	// TODO: can we do this with a core feed? we need it to go to the background and not block the connection.
	// if we do it as an enterprise feed/job, licensing aside, we'll need to pass the p.notificationSender thru the job system somehow which sounds shitty.
	//   would need to make the CAs send all data to CFs like in core, and make sure CF is on the right node and session etc?
	// otoh if we do "disown" the flow, how do we UNLISTEN it? on disconnect probably ctx is canceled or smth so that's ok...
	//
	// i think the last guy always runs on the local node. in core case that's the sql node, in enterprise case that's the job node.

	// does the ca usually send all data incl rows to the frontier?? no way
	//  but i think it do though, for core. then the frontier -> recv
	// so then for core feeds, do all CAs need to run on the local node? dont think so
	// so then they send their output to the recv/DistSQLReceiver thingy

	// TODO: getting single quotes around payloads somewhere

	stmt := &tree.CreateChangefeed{
		Targets: []tree.ChangefeedTarget{{TableName: tree.NewTableNameWithSchema("system", "public", "notifications")}},
		SinkURI: nil,
		Options: []tree.KVOption{
			// TODO: uncomment this when done testing
			// {Key: "initial_scan", Value: tree.NewStrVal("no")}, // DBG
			// TODO: change this to be a bool, then add a select query to filter down to the channel. or is there a more efficient way?
			{Key: "experimental_listen_channel", Value: tree.NewStrVal(n.ChannelName.String())},
		},
	}
	pn, err := p.maybePlanHook(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return pn, nil
}
