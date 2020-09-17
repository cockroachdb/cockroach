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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// crdbInternalSendNotice sends a notice.
// Note this is extracted to a different file to prevent churn on the pgwire
// test, which records line numbers.
func crdbInternalSendNotice(
	ctx *tree.EvalContext, severity string, msg string,
) (tree.Datum, error) {
	if ctx.ClientNoticeSender == nil {
		return nil, errors.AssertionFailedf("notice sender not set")
	}
	ctx.ClientNoticeSender.BufferClientNotice(
		ctx.Context,
		pgnotice.NewWithSeverityf(strings.ToUpper(severity), "%s", msg),
	)
	return tree.NewDInt(0), nil
}
