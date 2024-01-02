// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package srverrors_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestServerError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	pgError := pgerror.New(pgcode.OutOfMemory, "TestServerError.OutOfMemory")
	err := srverrors.ServerError(ctx, pgError)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details. Error Code: 53200", err.Error())

	err = srverrors.ServerError(ctx, err)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details. Error Code: 53200", err.Error())

	err = fmt.Errorf("random error that is not pgerror or grpcstatus")
	err = srverrors.ServerError(ctx, err)
	require.Equal(t, "rpc error: code = Internal desc = An internal server error has occurred. Please check your CockroachDB logs for more details.", err.Error())
}
