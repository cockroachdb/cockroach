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
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

type exportNode struct {
	optColumnsSlot

	source planNode

	// destination represents the destination URI for the export,
	// typically a directory
	destination string
	// fileNamePattern represents the file naming pattern for the
	// export, typically to be appended to the destination URI
	fileNamePattern string
	csvOpts         roachpb.CSVOptions
	chunkRows       int
	fileCompression execinfrapb.FileCompression
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
	exportOptionDelimiter   = "delimiter"
	exportOptionNullAs      = "nullas"
	exportOptionChunkRows   = "chunk_rows"
	exportOptionFileName    = "filename"
	exportOptionCompression = "compression"
)

var exportOptionExpectValues = map[string]KVStringOptValidate{
	exportOptionChunkRows:   KVStringOptRequireValue,
	exportOptionDelimiter:   KVStringOptRequireValue,
	exportOptionFileName:    KVStringOptRequireValue,
	exportOptionNullAs:      KVStringOptRequireValue,
	exportOptionCompression: KVStringOptRequireValue,
}

const exportChunkRowsDefault = 100000
const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"
const exportCompressionCodec = "gzip"

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

	destinationDatum, err := fileName.Eval(ef.planner.EvalContext())
	if err != nil {
		return nil, err
	}

	destination, ok := destinationDatum.(*tree.DString)
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

	chunkRows := exportChunkRowsDefault
	if override, ok := optVals[exportOptionChunkRows]; ok {
		chunkRows, err = strconv.Atoi(override)
		if err != nil {
			return nil, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
		}
		if chunkRows < 1 {
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid csv chunk size")
		}
	}

	// Check whenever compression is expected and extract compression codec name in case
	// of positive result
	var codec execinfrapb.FileCompression
	if name, ok := optVals[exportOptionCompression]; ok && len(name) != 0 {
		if strings.EqualFold(name, exportCompressionCodec) {
			codec = execinfrapb.FileCompression_Gzip
		} else {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"unsupported compression codec %s", name)
		}
	}

	exportID := ef.planner.stmt.QueryID.String()
	namePattern := fmt.Sprintf("export%s-%s", exportID, exportFilePatternDefault)

	return &exportNode{
		source:          input.(planNode),
		destination:     string(*destination),
		fileNamePattern: namePattern,
		csvOpts:         csvOpts,
		chunkRows:       chunkRows,
		fileCompression: codec,
	}, nil
}
