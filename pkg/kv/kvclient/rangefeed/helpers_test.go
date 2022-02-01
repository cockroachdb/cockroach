// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// NewDBAdapter allows tests to construct a dbAdapter.
var NewDBAdapter = newDBAdapter

// NewFactoryWithDB allows tests to construct a factory with an injected db.
var NewFactoryWithDB = newFactory

// KVDB forwards the definition of DB to tests.
type KVDB = DB

// ScanConfig forwards the definition of scanConfig to tests.
type ScanConfig = scanConfig

// ScanWithOptions is exposed for testing in order to call Scan with scanConfig
// extracted from the specified list of options.
func (dbc *dbAdapter) ScanWithOptions(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	opts ...Option,
) error {
	var c config
	initConfig(&c, opts)
	return dbc.Scan(ctx, spans, asOf, rowFn, c.scanConfig)
}
