// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errwrap

// ErrorFnFormatStringIndex contains functions that should be checked for improperly
// wrapped errors. The value is the index of the function parameter containing
// the format string. It is -1 if there is no format string parameter.
var ErrorFnFormatStringIndex = map[string]int{
	"errors.New": -1,

	"github.com/pkg/errors.New":  -1,
	"github.com/pkg/errors.Wrap": -1,

	"github.com/cockroachdb/errors.New":                        -1,
	"github.com/cockroachdb/errors.Error":                      -1,
	"github.com/cockroachdb/errors.NewWithDepth":               -1,
	"github.com/cockroachdb/errors.WithMessage":                -1,
	"github.com/cockroachdb/errors.Wrap":                       -1,
	"github.com/cockroachdb/errors.WrapWithDepth":              -1,
	"github.com/cockroachdb/errors.AssertionFailed":            -1,
	"github.com/cockroachdb/errors.HandledWithMessage":         -1,
	"github.com/cockroachdb/errors.HandledInDomainWithMessage": -1,

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.New": -1,

	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.New":                -1,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.NewWithIssue":       -1,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.NewWithIssueDetail": -1,

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire.newAdminShutdownErr": -1,

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.Wrap": -1,

	"(*github.com/cockroachdb/cockroach/pkg/parser/lexer).Error": -1,

	"fmt.Errorf": 0,

	"github.com/pkg/errors.Errorf": 0,
	"github.com/pkg/errors.Wrapf":  1,

	"github.com/cockroachdb/errors.Newf":                             0,
	"github.com/cockroachdb/errors.Errorf":                           0,
	"github.com/cockroachdb/errors.NewWithDepthf":                    1,
	"github.com/cockroachdb/errors.WithMessagef":                     1,
	"github.com/cockroachdb/errors.Wrapf":                            1,
	"github.com/cockroachdb/errors.WrapWithDepthf":                   2,
	"github.com/cockroachdb/errors.AssertionFailedf":                 0,
	"github.com/cockroachdb/errors.AssertionFailedWithDepthf":        1,
	"github.com/cockroachdb/errors.NewAssertionErrorWithWrappedErrf": 1,
	"github.com/cockroachdb/errors.WithSafeDetails":                  1,

	"github.com/cockroachdb/cockroach/pkg/roachpb.NewErrorf": 0,

	"github.com/cockroachdb/cockroach/pkg/sql/importer.makeRowErr": 3,
	"github.com/cockroachdb/cockroach/pkg/sql/importer.wrapRowErr": 4,

	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors.NewSyntaxErrorf":          0,
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors.NewDependentObjectErrorf": 0,

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree.decorateTypeCheckError": 1,

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder.unimplementedWithIssueDetailf": 2,

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.Newf":                1,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.NewWithDepthf":       2,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.DangerousStatementf": 0,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.Wrapf":               2,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror.WrapWithDepthf":      3,

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice.Newf":                                   0,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice.NewWithSeverityf":                       1,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase.NewProtocolViolationErrorf":           0,
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase.NewInvalidBinaryRepresentationErrorf": 0,

	"github.com/cockroachdb/cockroach/pkg/util/errorutil.UnexpectedWithIssueErrorf": 1,

	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.Newf":                  1,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.NewWithDepthf":         2,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.NewWithIssuef":         1,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.NewWithIssueDetailf":   2,
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented.unimplementedInternal": 3,

	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate.inputErrorf": 0,

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl.newErrorf": 1,
}
