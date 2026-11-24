// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goexectrace

import (
	"context"
	"encoding/json"
	rttrace "runtime/trace"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// execTraceMetadata is the JSON-serializable structure embedded into Go
// execution traces via runtime/trace.Log.
type execTraceMetadata struct {
	// TODO(jasonlmfong): consider adding TenantID for multi-tenant deployments.
	// The tenant ID is available at both call sites (server.go and tenant.go)
	// via roachpb.SystemTenantID and sqlCfg.TenantID respectively.
	Wallclock string         `json:"wallclock"`
	NodeID    roachpb.NodeID `json:"node_id"`
	Version   string         `json:"version"`
	Source    string         `json:"source"`
}

// NewLogMetadataFn returns a callback that embeds metadata into a Go execution
// trace via runtime/trace.Log. The metadata includes the current wall-clock
// time, node ID, build version, and the source of the trace capture. The
// returned function is safe to call from any goroutine.
func NewLogMetadataFn(idContainer *base.NodeIDContainer) func(ctx context.Context, source string) {
	return func(ctx context.Context, source string) {
		meta := execTraceMetadata{
			Wallclock: timeutil.Now().Format(time.RFC3339Nano),
			NodeID:    idContainer.Get(),
			Version:   build.GetInfo().Tag,
			Source:    source,
		}
		var payload string
		if b, err := json.Marshal(meta); err != nil {
			payload = "marshal error: " + err.Error()
		} else {
			payload = string(b)
		}
		rttrace.Log(ctx, "crdb.exectrace.meta", payload)
	}
}
