// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streaming

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// StreamAPIFactoryHook is the hook for the collection of APIs that streaming replication builtins support.
var StreamAPIFactoryHook func(apiName string, evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error)
