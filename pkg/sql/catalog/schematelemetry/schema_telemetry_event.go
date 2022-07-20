// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schematelemetry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// CollectClusterSchemaForTelemetry returns a projection of the cluster's SQL
// schema as of the provided system time, suitably filtered for the purposes of
// schema telemetry.
//
// This function is tested in the systemschema package.
func CollectClusterSchemaForTelemetry(
	ctx context.Context, cfg *sql.ExecutorConfig, asOf hlc.Timestamp,
) (events []eventpb.EventPayload, _ error) {
	// TODO(postamar): implement
	return nil, nil
}
