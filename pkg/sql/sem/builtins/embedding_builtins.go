// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/embedding/chunker"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vectorizer/content"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func init() {
	for k, v := range embeddingBuiltins {
		v.props.Category = builtinconstants.CategoryEmbedding
		v.props.AvailableOnPublicSchema = true
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
	for k, v := range embeddingGenerators {
		v.props.Category = builtinconstants.CategoryEmbedding
		v.props.AvailableOnPublicSchema = true
		const enforceClass = true
		registerBuiltin(k, v, tree.GeneratorClass, enforceClass)
	}
}

var embedChunksGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.String, types.PGVector},
	[]string{"chunk_seq", "chunk", "embedding"},
)

var embeddingGenerators = map[string]builtinDefinition{
	"embed_chunks": makeBuiltin(defProps(),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "text", Typ: types.String}},
			embedChunksGeneratorType,
			makeEmbedChunksGenerator,
			"Splits the input text into overlapping chunks, embeds each "+
				"chunk, and returns rows of (chunk_seq, chunk, embedding). "+
				"Uses the all-MiniLM-L6-v2 model (384 dimensions). Useful "+
				"for embedding long documents that exceed the model's token "+
				"limit.",
			volatility.Stable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "text", Typ: types.String},
				{Name: "model", Typ: types.String},
			},
			embedChunksGeneratorType,
			makeEmbedChunksGeneratorWithModel,
			"Splits the input text into overlapping chunks, embeds each "+
				"chunk using the specified model, and returns rows of "+
				"(chunk_seq, chunk, embedding). For remote models like "+
				"'openai/text-embedding-3-small', requires a matching "+
				"external connection.",
			volatility.Stable,
		),
	),
}

var embeddingBuiltins = map[string]builtinDefinition{
	"embed": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "text", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.PGVector),
			Fn: func(
				ctx context.Context, _ *eval.Context, args tree.Datums,
			) (tree.Datum, error) {
				eng, err := embedding.GetEngine()
				if err != nil {
					return nil, err
				}
				text := string(tree.MustBeDString(args[0]))
				vec, err := eng.Embed(ctx, text)
				if err != nil {
					return nil, err
				}
				return tree.NewDPGVector(vector.T(vec)), nil
			},
			Info: "Returns the vector embedding of the input text using the " +
				"all-MiniLM-L6-v2 model (384 dimensions). The model is " +
				"downloaded automatically on first use. Requires the ONNX " +
				"Runtime library (--embedding-libs).",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "text", Typ: types.String},
				{Name: "model", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.PGVector),
			Fn: func(
				ctx context.Context, evalCtx *eval.Context, args tree.Datums,
			) (tree.Datum, error) {
				text := string(tree.MustBeDString(args[0]))
				modelSpec := string(tree.MustBeDString(args[1]))
				embedder, err := resolveEmbedder(ctx, evalCtx, modelSpec)
				if err != nil {
					return nil, err
				}
				vec, err := embedder.Embed(ctx, text)
				if err != nil {
					return nil, err
				}
				return tree.NewDPGVector(vector.T(vec)), nil
			},
			Info: "Returns the vector embedding of the input text using the " +
				"specified model. For remote models like " +
				"'openai/text-embedding-3-small', requires a matching " +
				"external connection (CREATE EXTERNAL CONNECTION openai " +
				"AS 'https://api.openai.com/v1?api_key=sk-...').",
			Volatility: volatility.Stable,
		},
	),

	"read_uri": makeBuiltin(
		tree.FunctionProperties{DistsqlBlocklist: true},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "uri", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(
				ctx context.Context, evalCtx *eval.Context, args tree.Datums,
			) (tree.Datum, error) {
				uri := string(tree.MustBeDString(args[0]))
				data, err := evalCtx.Planner.ExternalReadFile(ctx, uri)
				if err != nil {
					return nil, errors.Wrap(err, "reading URI")
				}
				if int64(len(data)) > content.MaxFileSize {
					return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
						"file size %d bytes exceeds maximum %d bytes for read_uri",
						len(data), content.MaxFileSize)
				}
				text, err := content.ExtractText(data, uri)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(text), nil
			},
			Info: "Fetches file content from a cloud storage URI (s3://, gs://, " +
				"http://, nodelocal://) and returns extracted text. Supports " +
				"text-based formats (.txt, .md, .csv, .json, etc.). " +
				"Maximum file size: 64MB.",
			Volatility: volatility.Volatile,
		},
	),
}

// resolveEmbedder resolves a model specification into an Embedder.
// For local models (no "/" prefix), returns the global ONNX engine.
// For remote models like "openai/text-embedding-3-small", looks up
// the external connection named after the provider to get the
// connection URI, then constructs the remote embedder.
func resolveEmbedder(
	ctx context.Context, evalCtx *eval.Context, modelSpec string,
) (embedding.Embedder, error) {
	provider, _ := embedding.ParseModelSpec(modelSpec)
	if provider == "" {
		// Local model.
		return embedding.GetEngine()
	}

	// Remote model: look up connection URI from external connection.
	connURI, err := lookupExternalConnURI(ctx, evalCtx, provider)
	if err != nil {
		return nil, err
	}

	return embedding.ResolveRemoteEmbedder(modelSpec, connURI)
}

// lookupExternalConnURI queries system.external_connections for a
// connection with the given name and returns the raw URI string.
func lookupExternalConnURI(
	ctx context.Context, evalCtx *eval.Context, connectionName string,
) (string, error) {
	row, err := evalCtx.Planner.QueryRowEx(
		ctx,
		redact.Sprint("embed-lookup-external-connection"),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT connection_details FROM system.external_connections WHERE connection_name = $1",
		connectionName,
	)
	if err != nil {
		return "", errors.Wrap(err, "looking up external connection")
	}
	if row == nil {
		return "", pgerror.Newf(pgcode.UndefinedObject,
			"external connection %q not found; create it with: "+
				"CREATE EXTERNAL CONNECTION %s AS '...'",
			connectionName, connectionName)
	}

	// Deserialize the ConnectionDetails protobuf.
	detailsBytes := []byte(tree.MustBeDBytes(row[0]))
	var details connectionpb.ConnectionDetails
	if err := protoutil.Unmarshal(detailsBytes, &details); err != nil {
		return "", errors.Wrap(err, "decoding external connection details")
	}

	// Extract the URI from the SimpleURI variant.
	simpleURI, ok := details.Details.(*connectionpb.ConnectionDetails_SimpleURI)
	if !ok {
		return "", pgerror.Newf(pgcode.InvalidParameterValue,
			"external connection %q is not a URI-based connection", connectionName)
	}

	return simpleURI.SimpleURI.URI, nil
}

// makeEmbedChunksGenerator creates a ValueGenerator that chunks text
// and embeds each chunk using the local engine.
func makeEmbedChunksGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	text := string(tree.MustBeDString(args[0]))
	return &embedChunksGenerator{text: text}, nil
}

// makeEmbedChunksGeneratorWithModel creates a ValueGenerator that
// chunks text and embeds each chunk using the specified model.
func makeEmbedChunksGeneratorWithModel(
	_ context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	text := string(tree.MustBeDString(args[0]))
	modelSpec := string(tree.MustBeDString(args[1]))
	return &embedChunksGenerator{
		text:      text,
		modelSpec: modelSpec,
		evalCtx:   evalCtx,
	}, nil
}

// embedChunksGenerator implements eval.ValueGenerator for embed_chunks().
// It lazily chunks and embeds the text in Start, then yields one row
// per chunk via Next/Values.
type embedChunksGenerator struct {
	text      string
	modelSpec string
	evalCtx   *eval.Context
	chunks    []chunker.Chunk
	vecs      [][]float32
	idx       int
}

// ResolvedType implements eval.ValueGenerator.
func (g *embedChunksGenerator) ResolvedType() *types.T {
	return embedChunksGeneratorType
}

// Start implements eval.ValueGenerator. It chunks the text and embeds
// all chunks in a single batch.
func (g *embedChunksGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	if g.modelSpec == "" {
		return g.startLocal(ctx)
	}
	return g.startWithModel(ctx)
}

// startLocal uses the local ONNX engine with token-aware chunking.
func (g *embedChunksGenerator) startLocal(ctx context.Context) error {
	eng, err := embedding.GetEngine()
	if err != nil {
		return err
	}

	ch := chunker.NewChunker(eng.Tokenizer())
	g.chunks = ch.Chunk(g.text)

	texts := make([]string, len(g.chunks))
	for i, c := range g.chunks {
		texts[i] = c.Text
	}

	g.vecs, err = eng.EmbedBatch(ctx, texts)
	if err != nil {
		return err
	}

	g.idx = -1
	return nil
}

// startWithModel resolves the specified model and embeds. For remote
// models, the entire text is treated as a single chunk since the
// local tokenizer does not match the remote model's tokenizer.
// TODO(pradyum): Use per-model chunk sizes for remote models.
func (g *embedChunksGenerator) startWithModel(ctx context.Context) error {
	embedder, err := resolveEmbedder(ctx, g.evalCtx, g.modelSpec)
	if err != nil {
		return err
	}

	// For now, treat the entire text as a single chunk for remote
	// models. Per-model chunking is a future enhancement.
	g.chunks = []chunker.Chunk{{Text: g.text, SeqNum: 0}}
	vec, err := embedder.Embed(ctx, g.text)
	if err != nil {
		return err
	}
	g.vecs = [][]float32{vec}

	g.idx = -1
	return nil
}

// Close implements eval.ValueGenerator.
func (g *embedChunksGenerator) Close(_ context.Context) {}

// Next implements eval.ValueGenerator.
func (g *embedChunksGenerator) Next(_ context.Context) (bool, error) {
	g.idx++
	return g.idx < len(g.chunks), nil
}

// Values implements eval.ValueGenerator.
func (g *embedChunksGenerator) Values() (tree.Datums, error) {
	ch := g.chunks[g.idx]
	return tree.Datums{
		tree.NewDInt(tree.DInt(ch.SeqNum)),
		tree.NewDString(ch.Text),
		tree.NewDPGVector(vector.T(g.vecs[g.idx])),
	}, nil
}
