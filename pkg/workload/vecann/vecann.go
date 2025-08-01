// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadimpl"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// The schemas for vector tables.
const (
	vectorTableSchema = `(
		id INT8 DEFAULT (unique_rowid()) PRIMARY KEY,
		grp INT8 NOT NULL,
		vec VECTOR(%d) NOT NULL,
		VECTOR INDEX (grp, vec)
	)`
)

var randomSeed = workload.NewInt64RandomSeed()

func init() {
	workload.Register(vectorMeta)
}

var vectorMeta = workload.Meta{
	Name:        "vecann",
	Description: `VecAnn inserts vectors and performs approximate nearest-neighbor search queries.`,
	Details: `
	The vector workload inserts and queries vectors that represent text,
	images, or other media in high-dimensional space. When two vectors are
	close to one another in this space, it indicates their original	media
	are similar. Vectors are grouped together in separate K-means trees in
	the index according to a Zipfian distribution - if the first group has
	N vectors, then the second group has N / 2 vectors, the third N / 3,
	and so on.

	Datasets are generated from https://github.com/erikbern/ann-benchmarks
	and are stored in zipped .gob files in a GCP bucket:

		fashion-mnist-784-euclidean (60K vectors, 784 dims)
		gist-960-euclidean (1M vectors, 960 dims)
		random-s-100-euclidean (90K vectors, 100 dims)
		random-xs-20-euclidean (9K vectors, 20 dims)
		sift-128-euclidean (1M vectors, 128 dims)
		dbpedia-openai-100k-angular (100K vectors, 1536 dims)
	`,
	Version:    "1.0.0",
	RandomSeed: randomSeed,
	New: func() workload.Generator {
		g := &vectorWorkload{}
		g.flags.FlagSet = pflag.NewFlagSet("vector", pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			"dataset":          {},
			"cache-folder":     {},
			"max-results":      {RuntimeOnly: true},
			"search-beam-size": {RuntimeOnly: true},
			"batch-size":       {RuntimeOnly: true},
			"search-percent":   {RuntimeOnly: true},
			"num-groups":       {RuntimeOnly: true},
			"zipf-exponent":    {RuntimeOnly: true},
		}

		g.flags.StringVar(&g.datasetName, "dataset", "dbpedia-openai-100k-angular",
			"Name of dataset (e.g. dbpedia-openai-100k-angular).")
		g.flags.StringVar(&g.cacheFolder, "cache-folder", "",
			"Path to folder where downloaded datasets are cached.")
		g.flags.IntVar(&g.maxResults, "max-results", 10,
			"Number of results to return from vector searches.")
		g.flags.IntVar(&g.searchBeamSize, "search-beam-size", 32,
			"Beam size to use for vector searches.")
		g.flags.IntVar(&g.batchSize, "batch-size", 2,
			"Number of vectors to insert in a single batch.")
		g.flags.IntVar(&g.searchPercent, "search-percent", 0,
			"Percent (0-100) of operations that search for a vector.")
		g.flags.IntVar(&g.numGroups, "num-groups", 100,
			"Number of distinct vector groupings in the index.")
		g.flags.Float64Var(&g.zipfExponent, "zipf-exponent", 1.1,
			"Zipfian distribution exponent (>1 is steeper drop-off, <1 is more gradual).")

		randomSeed.AddFlag(&g.flags)

		// Always set the vector_search_beam_size session variable.
		g.connFlags = workload.NewConnFlags(&g.flags)
		g.connFlags.ConnVars = append(g.connFlags.ConnVars,
			fmt.Sprintf("vector_search_beam_size=%d", g.searchBeamSize))

		return g
	},
}

// vectorWorkload implements the methods required by the workload framework.
type vectorWorkload struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	datasetName    string
	cacheFolder    string
	maxResults     int
	searchBeamSize int
	batchSize      int
	searchPercent  int
	numGroups      int
	zipfExponent   float64

	tableName  string
	insertData Dataset
	searchData SearchDataset
	runner     workload.SQLRunner
	insertStmt workload.StmtHandle
	searchStmt workload.StmtHandle
}

// Meta implements the Generator interface.
func (*vectorWorkload) Meta() workload.Meta { return vectorMeta }

// Flags implements the Flagser interface.
func (vw *vectorWorkload) Flags() workload.Flags { return vw.flags }

// ConnFlags implements the ConnFlagser interface.
func (vw *vectorWorkload) ConnFlags() *workload.ConnFlags { return vw.connFlags }

// Tables implements the Generator interface.
func (vw *vectorWorkload) Tables() []workload.Table {
	tables := make([]workload.Table, 1)

	// Table for storing vectors (no initial rows).
	tables[0] = workload.Table{
		Name:   vw.tableName,
		Schema: fmt.Sprintf(vectorTableSchema, vw.searchData.Test.Dims),
	}

	return tables
}

// Hooks implements the Hookser interface.
func (vw *vectorWorkload) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if vw.maxResults <= 0 {
				return fmt.Errorf("--max-results must be a positive integer")
			}
			if vw.searchBeamSize <= 0 {
				return fmt.Errorf("--search-beam-size must be a positive integer")
			}
			if vw.batchSize <= 0 {
				return fmt.Errorf("--batch-size must be a positive integer")
			}
			if vw.searchPercent < 0 || vw.searchPercent > 100 {
				return fmt.Errorf("--search-percent must be between 0 and 100")
			}
			if vw.numGroups <= 0 {
				return fmt.Errorf("--num-groups must be a positive integer")
			}
			if vw.zipfExponent < 0 {
				return fmt.Errorf("--zipf-exponent cannot be less than zero")
			}

			return nil
		},

		PreCreate: func(*gosql.DB) error {
			return vw.loadDataset()
		},
	}
}

// Ops implements the Opser interface.
func (vw *vectorWorkload) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if err := vw.loadDataset(); err != nil {
		return workload.QueryLoad{}, err
	}

	// Build the vector insert query.
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf(`UPSERT INTO %s (grp, vec) VALUES`, vw.tableName))
	for i := range vw.batchSize {
		j := i * 2
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		fmt.Fprintf(&queryBuilder, ` ($%d, $%d)`, j+1, j+2)
	}
	vw.insertStmt = vw.runner.Define(queryBuilder.String())

	// Build the vector search query.
	queryBuilder.Reset()
	queryBuilder.WriteString(fmt.Sprintf(
		"SELECT id FROM %s WHERE grp = $1 ORDER BY vec <-> $2 LIMIT %d",
		vw.tableName, vw.maxResults))
	vw.searchStmt = vw.runner.Define(queryBuilder.String())

	// Initialize the runner.
	cfg := workload.NewMultiConnPoolCfgFromFlags(vw.connFlags)
	cfg.MaxTotalConnections = vw.connFlags.Concurrency + 1
	connPool, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, errors.Wrapf(err, "creating MultiConnPool")
	}
	err = vw.runner.Init(ctx, "vecann", connPool)
	if err != nil {
		return workload.QueryLoad{}, errors.Wrapf(err, "initializing SQL runner")
	}

	// Set up the workers.
	ql := workload.QueryLoad{}
	for i := range vw.connFlags.Concurrency {
		worker := &vectorWorker{
			workload: vw,
			hists:    reg.GetHandle(),
			rng:      rand.New(rand.NewPCG(uint64(randomSeed.Seed()+int64(i)), 0)),
		}
		worker.zipf, err = workloadimpl.NewZipfGenerator(
			worker.rng, 0, uint64(vw.numGroups)-1, vw.zipfExponent, false /* verbose */)
		if err != nil {
			return workload.QueryLoad{}, errors.Wrapf(err, "creating zipfian generator")
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}

	return ql, nil
}

// loadDataset downloads the dataset from the GCP bucket and reads it into
// memory.
func (vw *vectorWorkload) loadDataset() error {
	if vw.tableName != "" {
		// Dataset already loaded.
		return nil
	}

	lastDownloaded := int64(0)
	ctx := context.Background()
	loader := DatasetLoader{
		DatasetName: vw.datasetName,
		CacheFolder: vw.cacheFolder,
		OnProgress: func(ctx context.Context, format string, args ...any) {
			log.Infof(ctx, "%s", fmt.Sprintf(format, args...))
		},
		OnDownloadProgress: func(downloaded, total int64, elapsed time.Duration) {
			// Only log after each 10% progress.
			threshold := (total + 3) / 10
			if downloaded > lastDownloaded+threshold {
				lastDownloaded = downloaded
				log.Infof(ctx, "Downloaded %s / %s (%.0f%%) in %v",
					humanizeutil.IBytes(downloaded), humanizeutil.IBytes(total),
					(float64(downloaded)/float64(total))*100, elapsed.Truncate(time.Second))
			}
		},
	}

	if vw.searchPercent == 100 {
		// Only need to load the search data, not the insert data. Loading
		// the insert data can take 10+ seconds for large datasets.
		if err := loader.LoadForSearch(ctx); err != nil {
			return err
		}
	} else {
		if err := loader.Load(ctx); err != nil {
			return err
		}
	}
	vw.insertData = loader.Data
	vw.searchData = loader.SearchData

	datasetName := strings.ReplaceAll(vw.datasetName, "-", "_")
	vw.tableName = fmt.Sprintf("vectors_%s", datasetName)

	return nil
}

// vectorWorker represents a single worker executing the vecann workload.
type vectorWorker struct {
	workload *vectorWorkload
	hists    *histogram.Histograms
	rng      *rand.Rand
	zipf     *workloadimpl.ZipfGenerator
}

// run executes the vecann workload.
func (w *vectorWorker) run(ctx context.Context) error {
	var op string
	p := w.rng.IntN(100)
	switch {
	case p < w.workload.searchPercent:
		op = "search"
	default:
		op = "insert"
	}

	var err error
	start := timeutil.Now()

	switch op {
	case "insert":
		err = w.doInsert(ctx)

	case "search":
		err = w.doSearch(ctx)
	}

	elapsed := timeutil.Since(start)
	w.hists.Get(op).Record(elapsed)
	return err
}

// doInsert inserts a new vector into the vectors table.
func (w *vectorWorker) doInsert(ctx context.Context) error {
	// Select a group value from a zipfian distribution.
	group := w.genGroup()

	// Execute the insert query.
	args := make([]any, w.workload.batchSize*2)
	for i := range w.workload.batchSize {
		// Randomly select an insert vector from the test set.
		offset := w.rng.IntN(w.workload.insertData.Train.Count)
		vec := w.workload.insertData.Train.At(offset)

		j := i * 2
		args[j] = group
		args[j+1] = vec.String()
	}

	_, err := w.workload.insertStmt.Exec(ctx, args...)
	if err != nil {
		return errors.Wrapf(err, "executing insert")
	}

	return nil
}

// doSearch performs a vector similarity search query.
func (w *vectorWorker) doSearch(ctx context.Context) error {
	// Randomly select a search vector from the test set.
	offset := w.rng.IntN(w.workload.searchData.Test.Count)
	vec := w.workload.searchData.Test.At(offset)

	// Select a group value from a zipfian distribution.
	group := w.genGroup()

	// Execute the search query.
	rows, err := w.workload.searchStmt.Query(ctx, group, vec)
	if err != nil {
		return errors.Wrapf(err, "executing search")
	}
	defer rows.Close()
	for rows.Next() {
	}

	return rows.Err()
}

// genGroup randomly generates a group value, which identifies a K-means tree in
// the index that contains a distinct subset of vectors. Group values follow a
// zipfian distribution.
func (w *vectorWorker) genGroup() int {
	return int(w.zipf.Uint64())
}
