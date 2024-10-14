// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsql

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

var CheckClusterSupportsPLpgSQL = func(settings *cluster.Settings) error {
	return sqlerrors.NewCCLRequiredError(
		errors.New("using PL/pgSQL requires a CCL binary"),
	)
}
