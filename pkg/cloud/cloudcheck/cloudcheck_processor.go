// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudcheck

import (
	"context"
	"fmt"
	"io"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type result struct {
	ok        bool
	error     string
	readRate  int
	writeRate int
	canDelete bool
}

var flowTypes = []*types.T{types.Int, types.String, types.Bool, types.String, types.Int, types.Int, types.Bool}

func checkStorage(ctx context.Context, store cloud.ExternalStorage) (result, error) {
	const namePrefix = "cockroach-storage-test"
	filename := fmt.Sprintf("%s-%d", namePrefix, rand.Int())
	const chunkSize = 1 << 14    // 16kb
	const payloadSize = 32 << 20 // 16mb

	buf := make([]byte, chunkSize)
	_, _ = rand.Read(buf)

	var res result

	// First write a file and time how long it takes.
	beforeWrite := timeutil.Now()
	w, err := store.Writer(ctx, filename)
	if err != nil {
		return res, errors.Wrap(err, "opening writer")
	}
	defer w.Close()

	var wrote int
	for wrote < payloadSize {
		n, err := w.Write(buf)
		if err != nil {
			return res, errors.Wrap(err, "writing chunk")
		}
		wrote += n
	}
	if err := w.Close(); err != nil {
		return res, errors.Wrap(err, "closing after writing")
	}
	res.writeRate = int(float64(wrote) / timeutil.Since(beforeWrite).Seconds())

	// Now read the file back and time it.
	beforeRead := timeutil.Now()
	r, err := store.ReadFile(ctx, filename)
	if err != nil {
		return res, errors.Wrap(err, "opening reader")
	}
	defer r.Close(ctx)

	var read int
	for {
		n, err := r.Read(ctx, buf)
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, errors.Wrap(err, "reading content")
		}
	}
	res.readRate = int(float64(read) / timeutil.Since(beforeRead).Seconds())

	// TODO(dt, bardin): create N objects and list them?

	res.ok = true

	if err := store.Delete(ctx, filename); err != nil {
		return res, errors.Wrap(err, "deleting file")
	}
	res.canDelete = true
	return res, nil
}

type proc struct {
	execinfra.ProcessorBase
	spec execinfrapb.CloudStorageTestSpec
}

func newCloudCheckProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.CloudStorageTestSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	p := &proc{spec: spec}
	if err := p.Init(ctx, p, post, flowTypes, flowCtx, processorID, output, nil /* memMonitor */, execinfra.ProcStateOpts{}); err != nil {
		return nil, err
	}
	return p, nil
}

// Start is part of the RowSource interface.
func (p *proc) Start(ctx context.Context) {
	p.StartInternal(ctx, "cloudcheck")
}

// Next is part of the RowSource interface.
func (p *proc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}

	var res result
	store, err := p.FlowCtx.Cfg.ExternalStorageFromURI(p.Ctx(), p.spec.Location, p.FlowCtx.EvalCtx.SessionData().User())
	if err != nil {
		res.error = errors.Wrapf(err, "opening external storage location %q", p.spec.Location).Error()
	} else {
		res, err = checkStorage(p.Ctx(), store)
		if err != nil {
			res.error = err.Error()
		}
	}

	p.MoveToDraining(nil)
	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(p.EvalCtx.NodeID.SQLInstanceID()))),
		rowenc.DatumToEncDatum(types.String, tree.NewDString(p.EvalCtx.Locality.String())),
		rowenc.DatumToEncDatum(types.Bool, tree.MakeDBool(tree.DBool(res.ok))),
		rowenc.DatumToEncDatum(types.String, tree.NewDString(res.error)),
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.readRate))),
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.writeRate))),
		rowenc.DatumToEncDatum(types.Bool, tree.MakeDBool(tree.DBool(res.canDelete))),
	}, nil
}
func init() {
	rowexec.NewCloudStorageTestProcessor = newCloudCheckProcessor
}
