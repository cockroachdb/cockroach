// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type deleteBatchNode struct {
	delete *tree.Delete
}

var _ exec.Node = &deleteBatchNode{}
var _ planNode = &deleteBatchNode{}

func (d deleteBatchNode) startExec(runParams) error {
	del := d.delete
	var hasSize bool
	for i, param := range del.Batch.Params {
		switch param.(type) {
		case *tree.SizeBatchParam:
			if hasSize {
				return pgerror.Newf(pgcode.Syntax, "invalid parameter at index %d, SIZE already specified", i)
			}
			hasSize = true
		}
	}
	if hasSize {
		// TODO(ecwall): remove when DELETE BATCH is supported
		return pgerror.Newf(pgcode.Syntax, "DELETE BATCH (SIZE <size>) not implemented")
	}
	// TODO(ecwall): remove when DELETE BATCH is supported
	return pgerror.Newf(pgcode.Syntax, "DELETE BATCH not implemented")
}

func (d deleteBatchNode) Next(runParams) (bool, error) {
	return false, nil
}

func (d deleteBatchNode) Values() tree.Datums {
	return nil
}

func (d deleteBatchNode) Close(context.Context) {

}
