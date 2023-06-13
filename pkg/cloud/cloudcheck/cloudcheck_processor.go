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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type result struct {
	ok         bool
	error      string
	wroteBytes int64
	wroteTime  time.Duration
	readBytes  int64
	readTime   time.Duration
	canDelete  bool
}

var flowTypes = []*types.T{
	types.Int, types.String, // node and locality
	types.Bool, types.String, // ok and error
	types.Int, types.Int, // read bytes/nanos
	types.Int, types.Int, // wrote bytes/nanos
	types.Bool, // canDelete
}

func checkURI(
	ctx context.Context,
	opener cloud.ExternalStorageFromURIFactory,
	location string,
	username username.SQLUsername,
	params Params,
) result {
	ctxDone := ctx.Done()

	transferSize := params.TransferSize
	if transferSize == 0 {
		transferSize = 32 << 20
	}

	var total result

	start := timeutil.Now()
	for {
		select {
		case <-ctxDone:
			return total
		default:
		}

		store, err := opener(ctx, location, username)
		if err != nil {
			total.error = errors.Wrapf(err, "opening external storage").Error()
			return total
		}
		defer store.Close()

		res, err := checkStorage(ctx, store, transferSize)
		if err != nil {
			res.error = err.Error()
		}

		total.wroteBytes += res.wroteBytes
		total.wroteTime += res.wroteTime
		total.readBytes += res.readBytes
		total.readTime += res.readTime

		// We break on !total.ok below so setting it here overwrites zero-value.
		total.ok = res.ok
		total.error = res.error
		total.canDelete = res.canDelete

		if !total.ok || timeutil.Since(start) > params.MinDuration {
			break
		}
	}

	return total
}

func checkStorage(
	ctx context.Context, store cloud.ExternalStorage, transferSize int64,
) (result, error) {
	const namePrefix = "cockroach-storage-test"
	filename := fmt.Sprintf("%s-%d", namePrefix, rand.Int())
	chunkSize := int64(1 << 15) // 32kb
	if transferSize < chunkSize {
		chunkSize = transferSize
	}

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

	for res.wroteBytes < transferSize {
		n, err := w.Write(buf)
		if err != nil {
			return res, errors.Wrap(err, "writing chunk")
		}
		res.wroteBytes += int64(n)
	}
	if err := w.Close(); err != nil {
		return res, errors.Wrap(err, "closing after writing")
	}
	res.wroteTime = timeutil.Since(beforeWrite)

	// Now read the file back and time it.
	beforeRead := timeutil.Now()
	r, _, err := store.ReadFile(ctx, filename, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return res, errors.Wrap(err, "opening reader")
	}
	defer r.Close(ctx)

	for {
		n, err := r.Read(ctx, buf)
		res.readBytes += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, errors.Wrap(err, "reading content")
		}
	}
	res.readTime = timeutil.Since(beforeRead)

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
	spec    execinfrapb.CloudStorageTestSpec
	results chan result
}

func newCloudCheckProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.CloudStorageTestSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	p := &proc{spec: spec}
	if err := p.Init(ctx, p, post, flowTypes, flowCtx, processorID, nil /* memMonitor */, execinfra.ProcStateOpts{}); err != nil {
		return nil, err
	}
	return p, nil
}

// Start is part of the RowSource interface.
func (p *proc) Start(ctx context.Context) {
	p.StartInternal(ctx, "cloudcheck.proc")

	concurrency := int(p.spec.Params.Concurrency)
	if concurrency < 1 {
		concurrency = 1
	}

	p.results = make(chan result, concurrency)

	if err := p.FlowCtx.Stopper().RunAsyncTask(p.Ctx(), "cloudcheck.proc", func(ctx context.Context) {
		defer close(p.results)
		if err := ctxgroup.GroupWorkers(ctx, concurrency, func(ctx context.Context, _ int) error {
			select {
			case p.results <- checkURI(
				ctx,
				p.FlowCtx.Cfg.ExternalStorageFromURI,
				p.spec.Location,
				p.FlowCtx.EvalCtx.SessionData().User(),
				p.spec.Params,
			):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			p.MoveToDraining(err)
		}
	}); err != nil {
		p.MoveToDraining(err)
	}
}

// Next is part of the RowSource interface.
func (p *proc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}
	select {
	case <-p.Ctx().Done():
		p.MoveToDraining(p.Ctx().Err())
		return nil, p.DrainHelper()
	case res, more := <-p.results:
		if !more {
			p.MoveToDraining(nil)
			return nil, p.DrainHelper()
		}
		return rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(p.EvalCtx.NodeID.SQLInstanceID()))),
			rowenc.DatumToEncDatum(types.String, tree.NewDString(p.EvalCtx.Locality.String())),
			rowenc.DatumToEncDatum(types.Bool, tree.MakeDBool(tree.DBool(res.ok))),
			rowenc.DatumToEncDatum(types.String, tree.NewDString(res.error)),
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.readBytes))),
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.readTime))),
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.wroteBytes))),
			rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(res.wroteTime))),
			rowenc.DatumToEncDatum(types.Bool, tree.MakeDBool(tree.DBool(res.canDelete))),
		}, nil
	}
}

func init() {
	rowexec.NewCloudStorageTestProcessor = newCloudCheckProcessor
}
