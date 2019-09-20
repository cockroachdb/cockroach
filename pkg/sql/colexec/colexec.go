// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
)

// KVOp is implemented by OpNodes that need to perform KV operations. It
// contains the SetTxn method through which they get the transaction that they
// need to use.
type KVOp interface {
	SetTxn(context.Context, *client.Txn)
}

// SetTxn explores a tree of OpNodes, calling SetTxn on every node implementing
// KVOp. Everything above an UnorderedSynchronizer gets txn. Everything below
// gets a derived LeafTxn in case txn is a RootTxn.
//
// An UnorderedSynchronizer is the only node that introduces concurrency in a
// tree.
func SetTxn(ctx context.Context, n execinfra.OpNode, txn *client.Txn) {
	// Check if this node is forcing us to use switch to a LeafTxn for it and all
	// its children.
	if txn.Type() == client.RootTxn {
		// An UnorderedSynchronizer wants a LeafTxn for all its children because the
		// children execute concurrently (with one another and with other parts of
		// the flow).
		if _, ok := n.(*UnorderedSynchronizer); ok {
			meta := txn.GetTxnCoordMeta(ctx)
			txn = client.NewTxnWithCoordMeta(ctx, txn.DB(), txn.GatewayNodeID(), client.LeafTxn, meta)
		}
	}

	kvOp, ok := n.(KVOp)
	if ok {
		kvOp.SetTxn(ctx, txn)
	}
	for i := 0; i < n.ChildCount(); i++ {
		SetTxn(ctx, n.Child(i), txn)
	}
}
