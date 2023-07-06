// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// crdbInternalBufferNotice sends a notice that will be buffered until the
// connection is closed.
// Note this is extracted to a different file to prevent churn on the pgwire
// test, which records line numbers.
func crdbInternalBufferNotice(
	ctx context.Context, evalCtx *eval.Context, severity string, msg string,
) (tree.Datum, error) {
	if evalCtx.ClientNoticeSender == nil {
		return nil, errors.AssertionFailedf("notice sender not set")
	}
	evalCtx.ClientNoticeSender.BufferClientNotice(
		ctx,
		pgnotice.NewWithSeverityf(strings.ToUpper(severity), "%s", msg),
	)
	return tree.NewDInt(0), nil
}

// crdbInternalSendNotice immediately flushes a notice to the client.
func crdbInternalSendNotice(ctx context.Context, evalCtx *eval.Context, err error) error {
	if evalCtx.ClientNoticeSender == nil {
		return errors.AssertionFailedf("notice sender not set")
	}
	return evalCtx.ClientNoticeSender.SendClientNotice(ctx, pgnotice.Notice(err))
}
