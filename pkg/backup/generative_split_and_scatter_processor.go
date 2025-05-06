// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

const generativeSplitAndScatterProcessorName = "generativeSplitAndScatter"

var generativeSplitAndScatterOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router
	types.Bytes, // RestoreDataEntry bytes
}

// generativeSplitAndScatterProcessor is given a backup chain, whose manifests
// are specified in URIs and iteratively generates RestoreSpanEntries to be
// distributed across the cluster. Depending on which node the span ends up on,
// it forwards RestoreSpanEntry as bytes along with the key of the span on a
// row. It expects an output RangeRouter and before it emits each row, it
// updates the entry in the RangeRouter's map with the destination of the
// scatter.
type generativeSplitAndScatterProcessor struct {
	execinfra.ProcessorBase

	spec execinfrapb.GenerativeSplitAndScatterSpec

	// chunkSplitAndScatterers contain the splitAndScatterers for the group of
	// split and scatter workers that's responsible for splitting and scattering
	// the import span chunks. Each worker needs its own scatterer as one cannot
	// be used concurrently.
	chunkSplitAndScatterers []splitAndScatterer
	// chunkEntrySplitAndScatterers contain the splitAndScatterers for the group of
	// split workers that's responsible for making splits at each import span
	// entry. These scatterers only create splits for the start key of each import
	// span and do not perform any scatters.
	chunkEntrySplitAndScatterers []splitAndScatterer

	// cancelScatterAndWaitForWorker cancels the scatter goroutine and waits for
	// it to finish. It can be called multiple times.
	cancelScatterAndWaitForWorker func()

	doneScatterCh chan entryNode
	// A cache for routing datums, so only 1 is allocated per node.
	routingDatumCache routingDatumCache
	scatterErr        error
}

// scatteredChunk is the entries of a chunk of entries to process along with the
// node the chunk was scattered to.
type scatteredChunk struct {
	destination roachpb.NodeID
	entries     []execinfrapb.RestoreSpanEntry
}

type splitAndScatterer interface {
	// split issues a split request at the given key, which may be rewritten to
	// the RESTORE keyspace.
	split(ctx context.Context, codec keys.SQLCodec, splitKey roachpb.Key) error
	// scatter issues a scatter request at the given key. It returns the node ID
	// of where the range was scattered to.
	scatter(ctx context.Context, codec keys.SQLCodec, scatterKey roachpb.Key) (roachpb.NodeID, error)
}

type noopSplitAndScatterer struct {
	rng            *rand.Rand
	sqlInstanceIDs []int32
}

var _ splitAndScatterer = noopSplitAndScatterer{}

// split implements splitAndScatterer.
func (n noopSplitAndScatterer) split(_ context.Context, _ keys.SQLCodec, _ roachpb.Key) error {
	return nil
}

// scatter implements splitAndScatterer.
func (n noopSplitAndScatterer) scatter(
	_ context.Context, _ keys.SQLCodec, _ roachpb.Key,
) (roachpb.NodeID, error) {
	numInstances := len(n.sqlInstanceIDs)
	if numInstances == 0 {
		return 0, nil
	}
	idx := n.rng.Intn(numInstances)
	sqlInstanceID := n.sqlInstanceIDs[idx]
	return roachpb.NodeID(sqlInstanceID), nil
}

// dbSplitAndScatter is the production implementation of this processor's
// scatterer. It actually issues the split and scatter requests against the KV
// layer.
type dbSplitAndScatterer struct {
	db *kv.DB
	kr *KeyRewriter
}

var _ splitAndScatterer = dbSplitAndScatterer{}

func makeSplitAndScatterer(db *kv.DB, kr *KeyRewriter) splitAndScatterer {
	return dbSplitAndScatterer{db: db, kr: kr}
}

// split implements splitAndScatterer.
func (s dbSplitAndScatterer) split(
	ctx context.Context, codec keys.SQLCodec, splitKey roachpb.Key,
) error {
	if s.kr == nil {
		return errors.AssertionFailedf("KeyRewriter was not set when expected to be")
	}
	if s.db == nil {
		return errors.AssertionFailedf("split and scatterer's database was not set when expected")
	}

	expirationTime := s.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	newSplitKey, err := rewriteBackupSpanKey(codec, s.kr, splitKey)
	if err != nil {
		return err
	}
	if splitAt, err := keys.EnsureSafeSplitKey(newSplitKey); err != nil {
		// Ignore the error, not all keys are table keys.
	} else if len(splitAt) != 0 {
		newSplitKey = splitAt
	}
	log.VEventf(ctx, 1, "presplitting new key %+v", newSplitKey)
	if err := s.db.AdminSplit(ctx, newSplitKey, expirationTime); err != nil {
		return errors.Wrapf(err, "splitting key %s", newSplitKey)
	}

	return nil
}

// scatter implements splitAndScatterer.
func (s dbSplitAndScatterer) scatter(
	ctx context.Context, codec keys.SQLCodec, scatterKey roachpb.Key,
) (roachpb.NodeID, error) {
	if s.kr == nil {
		return 0, errors.AssertionFailedf("KeyRewriter was not set when expected to be")
	}
	if s.db == nil {
		return 0, errors.AssertionFailedf("split and scatterer's database was not set when expected")
	}

	newScatterKey, err := rewriteBackupSpanKey(codec, s.kr, scatterKey)
	if err != nil {
		return 0, err
	}
	if scatterAt, err := keys.EnsureSafeSplitKey(newScatterKey); err != nil {
		// Ignore the error, not all keys are table keys.
	} else if len(scatterAt) != 0 {
		newScatterKey = scatterAt
	}

	log.VEventf(ctx, 1, "scattering new key %+v", newScatterKey)
	req := &kvpb.AdminScatterRequest{
		RequestHeader: kvpb.RequestHeaderFromSpan(roachpb.Span{
			Key:    newScatterKey,
			EndKey: newScatterKey.Next(),
		}),
		// This is a bit of a hack, but it seems to be an effective one (see #36665
		// for graphs). As of the commit that added this, scatter is not very good
		// at actually balancing leases. This is likely for two reasons: 1) there's
		// almost certainly some regression in scatter's behavior, it used to work
		// much better and 2) scatter has to operate by balancing leases for all
		// ranges in a cluster, but in RESTORE, we really just want it to be
		// balancing the span being restored into.
		RandomizeLeases: true,
		MaxSize:         1, // don't scatter non-empty ranges on resume.
	}

	res, pErr := kv.SendWrapped(ctx, s.db.NonTransactionalSender(), req)
	if pErr != nil {
		// TODO(dt): typed error.
		if !strings.Contains(pErr.String(), "existing range size") {
			// TODO(pbardea): Unfortunately, Scatter is still too unreliable to
			// fail the RESTORE when Scatter fails. I'm uncomfortable that
			// this could break entirely and not start failing the tests,
			// but on the bright side, it doesn't affect correctness, only
			// throughput.
			log.Errorf(ctx, "failed to scatter span [%s,%s): %+v",
				newScatterKey, newScatterKey.Next(), pErr.GoError())
		}
		return 0, nil
	}

	return s.findDestination(res.(*kvpb.AdminScatterResponse)), nil
}

// findDestination returns the node ID of the node of the destination of the
// AdminScatter request. If the destination cannot be found, 0 is returned.
func (s dbSplitAndScatterer) findDestination(res *kvpb.AdminScatterResponse) roachpb.NodeID {
	if len(res.RangeInfos) > 0 {
		// If the lease is not populated, we return the 0 value anyway. We receive 1
		// RangeInfo per range that was scattered. Since we send a scatter request
		// to each range that we make, we are only interested in the first range,
		// which contains the key at which we're splitting and scattering.
		return res.RangeInfos[0].Lease.Replica.NodeID
	}

	return roachpb.NodeID(0)
}

func routingDatumsForSQLInstance(
	sqlInstanceID base.SQLInstanceID,
) (rowenc.EncDatum, rowenc.EncDatum) {
	routingBytes := roachpb.Key(fmt.Sprintf("node%d", sqlInstanceID))
	startDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes)))
	endDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes.Next())))
	return startDatum, endDatum
}

type entryNode struct {
	entry execinfrapb.RestoreSpanEntry
	node  roachpb.NodeID
}

var _ execinfra.Processor = &generativeSplitAndScatterProcessor{}

func newGenerativeSplitAndScatterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.GenerativeSplitAndScatterSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	db := flowCtx.Cfg.DB
	numNodes := int(spec.NumNodes)
	numChunkSplitAndScatterWorkers := numNodes
	// numEntrySplitWorkers is set to be 2 * numChunkSplitAndScatterWorkers in
	// order to keep up with the rate at which chunks are split and scattered.
	// TODO(rui): This tries to cover for a bad scatter by having 2 * the number
	// of nodes in the cluster. Does this knob need to be re-tuned?
	numEntrySplitWorkers := 2 * numChunkSplitAndScatterWorkers

	mkSplitAndScatterer := func() (splitAndScatterer, error) {
		if spec.ValidateOnly {
			rng, _ := randutil.NewPseudoRand()
			return noopSplitAndScatterer{rng, spec.SQLInstanceIDs}, nil
		}
		kr, err := MakeKeyRewriterFromRekeys(flowCtx.Codec(), spec.TableRekeys, spec.TenantRekeys,
			false /* restoreTenantFromStream */)
		if err != nil {
			return nil, err
		}
		return makeSplitAndScatterer(db.KV(), kr), nil
	}

	var chunkSplitAndScatterers []splitAndScatterer
	for i := 0; i < numChunkSplitAndScatterWorkers; i++ {
		scatterer, err := mkSplitAndScatterer()
		if err != nil {
			return nil, err
		}
		chunkSplitAndScatterers = append(chunkSplitAndScatterers, scatterer)
	}

	var chunkEntrySplitAndScatterers []splitAndScatterer
	for i := 0; i < numEntrySplitWorkers; i++ {
		scatterer, err := mkSplitAndScatterer()
		if err != nil {
			return nil, err
		}
		chunkEntrySplitAndScatterers = append(chunkEntrySplitAndScatterers, scatterer)
	}

	ssp := &generativeSplitAndScatterProcessor{
		spec:                         spec,
		chunkSplitAndScatterers:      chunkSplitAndScatterers,
		chunkEntrySplitAndScatterers: chunkEntrySplitAndScatterers,
		// There's not much science behind this sizing of doneScatterCh,
		// other than it's the max number of entries that can be processed
		// in parallel downstream. It has been verified ad-hoc that this
		// sizing does not bottleneck restore.
		doneScatterCh:     make(chan entryNode, numNodes*maxConcurrentRestoreWorkers),
		routingDatumCache: newRoutingDatumCache(),
	}
	if err := ssp.Init(ctx, ssp, post, generativeSplitAndScatterOutputTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil, // there are no inputs to drain
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ssp.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return ssp, nil
}

// Start is part of the RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", gssp.spec.JobID)
	ctx = gssp.StartInternal(ctx, generativeSplitAndScatterProcessorName)
	// Note that the loop over doneScatterCh in Next should prevent the goroutine
	// below from leaking when there are no errors. However, if that loop needs to
	// exit early, runSplitAndScatter's context will be canceled.
	scatterCtx, cancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	gssp.cancelScatterAndWaitForWorker = func() {
		cancel()
		<-workerDone
	}
	if err := gssp.FlowCtx.Stopper().RunAsyncTaskEx(scatterCtx, stop.TaskOpts{
		TaskName: "generativeSplitAndScatter-worker",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		gssp.scatterErr = runGenerativeSplitAndScatter(scatterCtx, gssp.FlowCtx, &gssp.spec, gssp.chunkSplitAndScatterers, gssp.chunkEntrySplitAndScatterers, gssp.doneScatterCh,
			&gssp.routingDatumCache)
		cancel()
		close(gssp.doneScatterCh)
		close(workerDone)
	}); err != nil {
		gssp.scatterErr = err
		cancel()
		close(workerDone)
	}
}

// Next implements the execinfra.RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) Next() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if gssp.State != execinfra.StateRunning {
		return nil, gssp.DrainHelper()
	}

	scatteredEntry, ok := <-gssp.doneScatterCh
	if ok {
		entry := scatteredEntry.entry
		entryBytes, err := protoutil.Marshal(&entry)
		if err != nil {
			gssp.MoveToDraining(err)
			return nil, gssp.DrainHelper()
		}

		// The routing datums informs the router which output stream should be used.
		routingDatum, ok := gssp.routingDatumCache.getRoutingDatum(scatteredEntry.node)
		if !ok {
			routingDatum, _ = routingDatumsForSQLInstance(base.SQLInstanceID(scatteredEntry.node))
			gssp.routingDatumCache.putRoutingDatum(scatteredEntry.node, routingDatum)
		}

		row := rowenc.EncDatumRow{
			routingDatum,
			rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
		}
		return row, nil
	}

	if gssp.scatterErr != nil {
		gssp.MoveToDraining(gssp.scatterErr)
		return nil, gssp.DrainHelper()
	}

	gssp.MoveToDraining(nil /* error */)
	return nil, gssp.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (gssp *generativeSplitAndScatterProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	gssp.close()
}

// close stops the production workers. This needs to be called if the consumer
// runs into an error and stops consuming scattered entries to make sure we
// don't leak goroutines.
func (gssp *generativeSplitAndScatterProcessor) close() {
	if gssp.cancelScatterAndWaitForWorker != nil {
		gssp.cancelScatterAndWaitForWorker()
	}
	gssp.InternalClose()
}

func makeBackupMetadata(
	ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.GenerativeSplitAndScatterSpec,
) ([]backuppb.BackupManifest, backupinfo.LayerToBackupManifestFileIterFactory, error) {

	execCfg := flowCtx.Cfg.ExecutorConfig.(*sql.ExecutorConfig)
	memAcc := flowCtx.EvalCtx.Planner.Mon().MakeBoundAccount()
	defer memAcc.Close(ctx)

	kmsEnv := backupencryption.MakeBackupKMSEnv(execCfg.Settings, &execCfg.ExternalIODirConfig,
		execCfg.InternalDB, spec.User())

	backupManifests, _, err := backupinfo.LoadBackupManifestsAtTime(ctx, &memAcc, spec.URIs,
		spec.User(), execCfg.DistSQLSrv.ExternalStorageFromURI, spec.Encryption, &kmsEnv, spec.EndTime)
	if err != nil {
		return nil, nil, err
	}

	layerToBackupManifestFileIterFactory, err := backupinfo.GetBackupManifestIterFactories(ctx, execCfg.DistSQLSrv.ExternalStorage,
		backupManifests, spec.Encryption, &kmsEnv)
	if err != nil {
		return nil, nil, err
	}

	return backupManifests, layerToBackupManifestFileIterFactory, nil
}

type restoreEntryChunk struct {
	entries  []execinfrapb.RestoreSpanEntry
	splitKey roachpb.Key
}

func runGenerativeSplitAndScatter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.GenerativeSplitAndScatterSpec,
	chunkSplitAndScatterers []splitAndScatterer,
	chunkEntrySplitAndScatterers []splitAndScatterer,
	doneScatterCh chan<- entryNode,
	cache *routingDatumCache,
) error {
	log.Infof(ctx, "Running generative split and scatter with %d total spans, %d chunk size, %d nodes",
		spec.NumEntries, spec.ChunkSize, spec.NumNodes)
	g := ctxgroup.WithContext(ctx)

	chunkSplitAndScatterWorkers := len(chunkSplitAndScatterers)
	restoreSpanEntriesCh := make(chan execinfrapb.RestoreSpanEntry, chunkSplitAndScatterWorkers*int(spec.ChunkSize))

	// This goroutine generates import spans one at a time and sends them to
	// restoreSpanEntriesCh.
	g.GoCtx(func(ctx context.Context) error {
		defer close(restoreSpanEntriesCh)

		backups, layerToFileIterFactory, err := makeBackupMetadata(ctx,
			flowCtx, spec)
		if err != nil {
			return errors.Wrap(err, "making backup metadata")
		}
		introducedSpanFrontier, err := createIntroducedSpanFrontier(backups, spec.EndTime)
		if err != nil {
			return errors.Wrap(err, "making introduced span frontier")
		}
		defer introducedSpanFrontier.Release()

		backupLocalityMap, err := makeBackupLocalityMap(spec.BackupLocalityInfo, spec.User())
		if err != nil {
			return errors.Wrap(err, "making backup locality map")
		}

		filter, err := makeSpanCoveringFilter(
			spec.Spans,
			spec.CheckpointedSpans,
			introducedSpanFrontier,
			spec.TargetSize,
			spec.MaxFileCount)
		if err != nil {
			return errors.Wrap(err, "failed to make span covering filter")
		}
		defer filter.close()

		var fsc fileSpanComparator = &inclusiveEndKeyComparator{}
		if spec.ExclusiveFileSpanComparison {
			fsc = &exclusiveEndKeyComparator{}
		}

		return errors.Wrap(generateAndSendImportSpans(
			ctx,
			spec.Spans,
			backups,
			layerToFileIterFactory,
			backupLocalityMap,
			filter,
			fsc,
			restoreSpanEntriesCh,
		), "generating and sending import spans")
	})

	restoreEntryChunksCh := make(chan restoreEntryChunk, chunkSplitAndScatterWorkers)

	// This goroutine takes the import spans off of restoreSpanEntriesCh and
	// groups them into chunks of spec.ChunkSize. These chunks are then sent to
	// restoreEntryChunksCh.
	g.GoCtx(func(ctx context.Context) error {
		defer close(restoreEntryChunksCh)

		var idx int64
		var chunk restoreEntryChunk
		for entry := range restoreSpanEntriesCh {
			entry.ProgressIdx = idx
			idx++
			if len(chunk.entries) == int(spec.ChunkSize) {
				chunk.splitKey = entry.Span.Key
				select {
				case <-ctx.Done():
					return errors.Wrap(ctx.Err(), "grouping restore span entries into chunks")
				case restoreEntryChunksCh <- chunk:
				}
				chunk = restoreEntryChunk{}
			}
			chunk.entries = append(chunk.entries, entry)
		}

		if len(chunk.entries) > 0 {
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "sending restore entry chunk")
			case restoreEntryChunksCh <- chunk:
			}
		}
		return nil
	})

	importSpanChunksCh := make(chan scatteredChunk, chunkSplitAndScatterWorkers*2)
	g.GoCtx(func(ctx context.Context) error {
		defer close(importSpanChunksCh)
		// This group of goroutines processes the chunks from restoreEntryChunksCh.
		// For each chunk, a split is created at the start key of the next chunk. The
		// current chunk is then scattered, and the chunk with its destination is
		// passed to importSpanChunksCh.
		g2 := ctxgroup.WithContext(ctx)
		for worker := 0; worker < chunkSplitAndScatterWorkers; worker++ {
			worker := worker
			g2.GoCtx(func(ctx context.Context) error {
				hash := fnv.New32a()

				// Chunks' leaseholders should be randomly placed throughout the
				// cluster.
				for importSpanChunk := range restoreEntryChunksCh {
					scatterKey := importSpanChunk.entries[0].Span.Key
					if !importSpanChunk.splitKey.Equal(roachpb.Key{}) {
						// Split at the start of the next chunk, to partition off a
						// prefix of the space to scatter.
						if err := chunkSplitAndScatterers[worker].split(ctx, flowCtx.Codec(), importSpanChunk.splitKey); err != nil {
							return err
						}
					}
					chunkDestination, err := chunkSplitAndScatterers[worker].scatter(ctx, flowCtx.Codec(), scatterKey)
					if err != nil {
						return err
					}
					if chunkDestination == 0 {
						// If scatter failed to find a node for range ingestion, route the
						// range to a random node that has already been scattered to so far.
						// The random node is chosen by hashing the scatter key.
						if nodeID, ok := flowCtx.NodeID.OptionalNodeID(); ok {
							cachedNodeIDs := cache.cachedNodeIDs()
							if len(cachedNodeIDs) > 0 {
								hash.Reset()
								if _, err := hash.Write(scatterKey); err != nil {
									log.Warningf(ctx, "scatter returned node 0. Route span starting at %s to current node %v because of hash error: %v",
										scatterKey, nodeID, err)
								} else {
									hashedKey := int(hash.Sum32())
									nodeID = cachedNodeIDs[hashedKey%len(cachedNodeIDs)]
								}

								log.Warningf(ctx, "scatter returned node 0. "+
									"Random route span starting at %s node %v", scatterKey, nodeID)
							} else {
								log.Warningf(ctx, "scatter returned node 0. "+
									"Route span starting at %s to current node %v", scatterKey, nodeID)
							}
							chunkDestination = nodeID
						} else {
							// TODO(rui): OptionalNodeID only returns a node if the sql server runs
							// in the same process as the kv server (e.g., not serverless). Figure
							// out how to handle this error in serverless restore.
							log.Warningf(ctx, "scatter returned node 0. "+
								"Route span starting at %s to default stream", scatterKey)
						}
					}

					if _, ok := flowCtx.NodeID.OptionalNodeID(); !ok {
						// If a seperate process tenant is running restore, the nodeID
						// returned by scatter does not map identically to a sqlInstanceID;
						// thus, the processor must randomly choose a sqlInstanceID to route
						// the chunk to.
						//
						// TODO(msbutler): it is unfortunate that all logic after scatter
						// operates on a NodeID object. The logic should use SQLInstanceID's
						// as these chunks are routed to DistSQL processors running on sql
						// servers.
						if len(spec.SQLInstanceIDs) > 0 {
							chunkDestination = roachpb.NodeID(spec.SQLInstanceIDs[rand.Intn(len(spec.SQLInstanceIDs))])
						} else {
							chunkDestination = roachpb.NodeID(0)
						}
					}

					sc := scatteredChunk{
						destination: chunkDestination,
						entries:     importSpanChunk.entries,
					}

					select {
					case <-ctx.Done():
						return errors.Wrap(ctx.Err(), "sending scattered chunk")
					case importSpanChunksCh <- sc:
					}
				}
				return nil
			})
		}

		// This goroutine waits for the chunkSplitAndScatter workers to finish so that
		// it can close importSpanChunksCh.
		return errors.Wrap(g2.Wait(), "waiting for chunkSplitAndScatter workers")
	})

	// This group of goroutines takes chunks that have already been split and
	// scattered by the previous worker group. These workers create splits at the
	// start key of the span of every entry of every chunk. After a chunk has been
	// processed, it is passed to doneScatterCh to signal that the chunk has gone
	// through the entire split and scatter process.
	for worker := 0; worker < len(chunkEntrySplitAndScatterers); worker++ {
		worker := worker
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				chunkDestination := importSpanChunk.destination
				for i, importEntry := range importSpanChunk.entries {
					nextChunkIdx := i + 1

					log.VInfof(ctx, 2, "processing a span [%s,%s)", importEntry.Span.Key, importEntry.Span.EndKey)
					var splitKey roachpb.Key
					if nextChunkIdx < len(importSpanChunk.entries) {
						// Split at the next entry.
						splitKey = importSpanChunk.entries[nextChunkIdx].Span.Key
						if err := chunkEntrySplitAndScatterers[worker].split(ctx, flowCtx.Codec(), splitKey); err != nil {
							return err
						}
					}

					scatteredEntry := entryNode{
						entry: importEntry,
						node:  chunkDestination,
					}

					if restoreKnobs, ok := flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
						if restoreKnobs.RunAfterSplitAndScatteringEntry != nil {
							restoreKnobs.RunAfterSplitAndScatteringEntry(ctx)
						}
					}

					select {
					case <-ctx.Done():
						return errors.Wrap(ctx.Err(), "sending scattered entry")
					case doneScatterCh <- scatteredEntry:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

type routingDatumCache struct {
	syncutil.Mutex
	cache   map[roachpb.NodeID]rowenc.EncDatum
	nodeIDs []roachpb.NodeID
}

func (c *routingDatumCache) getRoutingDatum(nodeID roachpb.NodeID) (rowenc.EncDatum, bool) {
	c.Lock()
	defer c.Unlock()
	d, ok := c.cache[nodeID]
	return d, ok
}

func (c *routingDatumCache) putRoutingDatum(nodeID roachpb.NodeID, datum rowenc.EncDatum) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.cache[nodeID]; !ok {
		c.nodeIDs = append(c.nodeIDs, nodeID)
	}
	c.cache[nodeID] = datum
}

func (c *routingDatumCache) cachedNodeIDs() []roachpb.NodeID {
	c.Lock()
	defer c.Unlock()
	return c.nodeIDs
}

func newRoutingDatumCache() routingDatumCache {
	return routingDatumCache{
		cache: make(map[roachpb.NodeID]rowenc.EncDatum),
	}
}

var splitAndScatterOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router
	types.Bytes, // RestoreDataEntry bytes
}

// routingSpanForSQLInstance provides the mapping to be used during distsql planning
// when setting up the output router.
func routingSpanForSQLInstance(sqlInstanceID base.SQLInstanceID) ([]byte, []byte, error) {
	var alloc tree.DatumAlloc
	startDatum, endDatum := routingDatumsForSQLInstance(sqlInstanceID)

	startBytes, endBytes := make([]byte, 0), make([]byte, 0)
	startBytes, err := startDatum.Encode(splitAndScatterOutputTypes[0], &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, startBytes)
	if err != nil {
		return nil, nil, err
	}
	endBytes, err = endDatum.Encode(splitAndScatterOutputTypes[0], &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, endBytes)
	if err != nil {
		return nil, nil, err
	}
	return startBytes, endBytes, nil
}

func init() {
	rowexec.NewGenerativeSplitAndScatterProcessor = newGenerativeSplitAndScatterProcessor
}
