package systembench

// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
