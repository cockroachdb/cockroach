// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colplan

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execinfrapb"
)

type vectorizedInboundStreamHandler struct {
	*colrpc.Inbox
}

var _ execinfra.InboundStreamHandler = vectorizedInboundStreamHandler{}

func (s vectorizedInboundStreamHandler) Run(
	ctx context.Context,
	stream execinfrapb.DistSQL_FlowStreamServer,
	_ *execinfrapb.ProducerMessage,
	_ execinfra.Flow,
) error {
	return s.RunWithStream(ctx, stream)
}

func (s vectorizedInboundStreamHandler) Timeout(err error) {
	s.Timeout(err)
}
