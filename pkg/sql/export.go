// Copyright 2019 The Cockroach Authors.
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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

type exportNode struct {
	optColumnsSlot

	source planNode

	fileName  string
	csvOpts   roachpb.CSVOptions
	chunkSize int
}

func (e *exportNode) startExec(params runParams) error {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Next(params runParams) (bool, error) {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Values() tree.Datums {
	panic("exportNode cannot be run in local mode")
}

func (e *exportNode) Close(ctx context.Context) {
	e.source.Close(ctx)
}

const (
	exportOptionDelimiter = "delimiter"
	exportOptionNullAs    = "nullas"
	exportOptionChunkSize = "chunk_rows"
	exportOptionFileName  = "filename"
)

var exportOptionExpectValues = map[string]KVStringOptValidate{
	exportOptionChunkSize: KVStringOptRequireValue,
	exportOptionDelimiter: KVStringOptRequireValue,
	exportOptionFileName:  KVStringOptRequireValue,
	exportOptionNullAs:    KVStringOptRequireValue,
}

const exportChunkSizeDefault = 100000
const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// ConstructExport is part of the exec.Factory interface.
func (ef *execFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	if !ef.planner.ExtendedEvalContext().TxnImplicit {
		return nil, errors.Errorf("EXPORT cannot be used inside a transaction")
	}

	if fileFormat != "CSV" {
		return nil, errors.Errorf("unsupported export format: %q", fileFormat)
	}

	fileNameDatum, err := fileName.Eval(ef.planner.EvalContext())
	if err != nil {
		return nil, err
	}
	fileNameStr, ok := fileNameDatum.(*tree.DString)
	if !ok {
		return nil, errors.Errorf("expected string value for the file location")
	}

	optVals, err := evalStringOptions(ef.planner.EvalContext(), options, exportOptionExpectValues)
	if err != nil {
		return nil, err
	}

	csvOpts := roachpb.CSVOptions{}

	if override, ok := optVals[exportOptionDelimiter]; ok {
		csvOpts.Comma, err = util.GetSingleRune(override)
		if err != nil {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid delimiter")
		}
	}

	if override, ok := optVals[exportOptionNullAs]; ok {
		csvOpts.NullEncoding = &override
	}

	chunkSize := exportChunkSizeDefault
	if override, ok := optVals[exportOptionChunkSize]; ok {
		chunkSize, err = strconv.Atoi(override)
		if err != nil {
			return nil, pgerror.New(pgcode.InvalidParameterValue, err.Error())
		}
		if chunkSize < 1 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid csv chunk size")
		}
	}

	return &exportNode{
		source:    input.(planNode),
		fileName:  string(*fileNameStr),
		csvOpts:   csvOpts,
		chunkSize: chunkSize,
	}, nil
}
