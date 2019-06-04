package systembench

// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
