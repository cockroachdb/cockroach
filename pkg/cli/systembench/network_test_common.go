package systembench

// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cli/systembench/systembenchpb"
)

type pinger struct {
	payload []byte
}

func (p *pinger) Ping(
	_ context.Context, req *systembench.PingRequest,
) (*systembench.PingResponse, error) {
	return &systembench.PingResponse{Payload: p.payload}, nil
}

func newPinger() *pinger {
	payload := make([]byte, serverPayload)
	_, _ = rand.Read(payload)
	return &pinger{payload: payload}
}
