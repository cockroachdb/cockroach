// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package vectorizerjob implements the resumer for the vectorizer background
// job. Each run processes a batch of rows from the source table that do not
// yet have embeddings in the companion table, generates embeddings using the
// embedding engine, and inserts them.
package vectorizerjob

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/vectorizer/content"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type vectorizerResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*vectorizerResumer)(nil)

// Resume implements the jobs.Resumer interface. It finds rows in the source
// table missing embeddings, generates them, and inserts into the companion
// table.
func (r *vectorizerResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	details := r.job.Details().(jobspb.VectorizerDetails)
	tableID := details.TableID

	// Read the vectorizer configuration from the table descriptor.
	var sourceColumns []string
	var tmpl string
	var model string
	var connectionName string
	var loadingMode string
	var inputType catpb.VectorizerInputType
	var batchSize int64
	var companionTableName tree.TableName
	var sourceTableName tree.TableName
	var pkColNames []string

	if err := execCfg.InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		tableDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return errors.Wrapf(err, "vectorizer: resolving table %d", tableID)
		}

		vectorizerCfg := tableDesc.TableDesc().Vectorizer
		if vectorizerCfg == nil {
			return errors.Newf(
				"vectorizer: table %d has no vectorizer configured", tableID,
			)
		}

		sourceColumns = vectorizerCfg.SourceColumns
		tmpl = vectorizerCfg.Template
		model = vectorizerCfg.Model
		connectionName = vectorizerCfg.ConnectionName
		loadingMode = vectorizerCfg.LoadingMode
		inputType = vectorizerCfg.InputType
		batchSize = vectorizerCfg.BatchSize
		if batchSize <= 0 {
			batchSize = 64
		}

		// Look up the source table name.
		tn, err := descs.GetObjectName(
			ctx, txn.KV(), txn.Descriptors(), tableDesc,
		)
		if err != nil {
			return errors.Wrap(err, "vectorizer: resolving source table name")
		}
		sourceTableName = tree.MakeTableNameWithSchema(
			tree.Name(tn.Catalog()),
			tree.Name(tn.Schema()),
			tree.Name(tn.Object()),
		)

		// Look up the companion table name.
		companionDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, vectorizerCfg.EmbeddingTableID)
		if err != nil {
			return errors.Wrap(err, "vectorizer: resolving companion table")
		}
		cn, err := descs.GetObjectName(
			ctx, txn.KV(), txn.Descriptors(), companionDesc,
		)
		if err != nil {
			return errors.Wrap(
				err, "vectorizer: resolving companion table name",
			)
		}
		companionTableName = tree.MakeTableNameWithSchema(
			tree.Name(cn.Catalog()),
			tree.Name(cn.Schema()),
			tree.Name(cn.Object()),
		)

		// Get PK column names from source table.
		pkIdx := tableDesc.GetPrimaryIndex()
		pkColNames = make([]string, pkIdx.NumKeyColumns())
		for i := range pkColNames {
			pkColNames[i] = pkIdx.GetKeyColumnName(i)
		}
		return nil
	}); err != nil {
		return err
	}

	// Build shared column lists for queries.
	var selectCols []string
	for _, pk := range pkColNames {
		selectCols = append(selectCols,
			fmt.Sprintf("s.%s", tree.NameString(pk)))
	}
	for _, col := range sourceColumns {
		selectCols = append(selectCols,
			fmt.Sprintf("s.%s", tree.NameString(col)))
	}

	var joinConds []string
	for _, pk := range pkColNames {
		joinConds = append(joinConds, fmt.Sprintf(
			"s.%s = e.%s",
			tree.NameString(pk),
			tree.NameString("source_"+pk),
		))
	}

	numPKCols := len(pkColNames)

	// Resolve the embedder: local engine or remote provider.
	embedder, err := resolveEmbedder(ctx, execCfg, model, connectionName)
	if err != nil {
		return err
	}

	var totalProcessed int64

	// Phase 1: Find rows missing embeddings (anti-join).
	//
	// SELECT s.<pk_cols>, s.<source_cols>
	// FROM <source_table> AS s
	// LEFT JOIN <companion_table> AS e
	//   ON s.<pk> = e.source_<pk>
	// WHERE e.source_<pk[0]> IS NULL
	// LIMIT <batch_size>
	missingSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS s LEFT JOIN %s AS e ON %s "+
			"WHERE e.%s IS NULL LIMIT %d",
		strings.Join(selectCols, ", "),
		sourceTableName.String(),
		companionTableName.String(),
		strings.Join(joinConds, " AND "),
		tree.NameString("source_"+pkColNames[0]),
		batchSize,
	)

	missingRows, err := execCfg.InternalDB.Executor().QueryBufferedEx(
		ctx, "vectorizer-select-missing", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		missingSQL,
	)
	if err != nil {
		return errors.Wrap(err, "vectorizer: querying missing rows")
	}

	if len(missingRows) > 0 {
		log.Ops.Infof(ctx,
			"vectorizer: embedding %d missing rows for table %d",
			len(missingRows), tableID)

		inserted, err := embedAndInsertRows(
			ctx, execCfg, embedder, missingRows,
			numPKCols, pkColNames, sourceColumns, tmpl,
			companionTableName, loadingMode, inputType,
			false, /* isUpdate */
		)
		if err != nil {
			return err
		}
		totalProcessed += inserted
	}

	// Phase 2: Find stale rows where source data changed after
	// embedding. Uses the MVCC timestamp of each source row compared
	// against last_embedded_at in the companion table.
	//
	// SELECT s.<pk_cols>, s.<source_cols>
	// FROM <source_table> AS s
	// JOIN <companion_table> AS e
	//   ON s.<pk> = e.source_<pk>
	// WHERE s.crdb_internal_mvcc_timestamp > e.last_embedded_at::DECIMAL * 1e9
	// GROUP BY s.<pk_cols>, s.<source_cols>
	// LIMIT <batch_size>
	var groupByCols []string
	groupByCols = append(groupByCols, selectCols...)

	staleSQL := fmt.Sprintf(
		"SELECT %s FROM %s AS s JOIN %s AS e ON %s "+
			"WHERE s.crdb_internal_mvcc_timestamp > e.last_embedded_at::DECIMAL * 1e9 "+
			"GROUP BY %s LIMIT %d",
		strings.Join(selectCols, ", "),
		sourceTableName.String(),
		companionTableName.String(),
		strings.Join(joinConds, " AND "),
		strings.Join(groupByCols, ", "),
		batchSize,
	)

	staleRows, err := execCfg.InternalDB.Executor().QueryBufferedEx(
		ctx, "vectorizer-select-stale", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		staleSQL,
	)
	if err != nil {
		return errors.Wrap(err, "vectorizer: querying stale rows")
	}

	if len(staleRows) > 0 {
		log.Ops.Infof(ctx,
			"vectorizer: re-embedding %d stale rows for table %d",
			len(staleRows), tableID)

		updated, err := embedAndInsertRows(
			ctx, execCfg, embedder, staleRows,
			numPKCols, pkColNames, sourceColumns, tmpl,
			companionTableName, loadingMode, inputType,
			true, /* isUpdate */
		)
		if err != nil {
			return err
		}
		totalProcessed += updated
	}

	if totalProcessed == 0 {
		log.Ops.Infof(ctx,
			"vectorizer: no pending rows for table %d", tableID)
	} else {
		log.Ops.Infof(ctx,
			"vectorizer: processed %d rows for table %d",
			totalProcessed, tableID)
	}

	// Update job progress.
	return r.job.NoTxn().FractionProgressed(ctx, func(
		ctx context.Context, details jobspb.ProgressDetails,
	) float32 {
		prog := details.(*jobspb.Progress_Vectorizer).Vectorizer
		prog.RowsProcessed += totalProcessed
		return 1.0 // Single-batch job, always complete.
	})
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *vectorizerResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *vectorizerResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

// resolveEmbedder returns the appropriate Embedder for the given model
// spec. For local models it returns the ONNX engine; for remote models
// it looks up the external connection URI and constructs a remote client.
func resolveEmbedder(
	ctx context.Context, execCfg *sql.ExecutorConfig, model string, connectionName string,
) (embedding.Embedder, error) {
	provider, _ := embedding.ParseModelSpec(model)
	if provider == "" {
		eng, err := embedding.GetEngine()
		if err != nil {
			return nil, jobs.MarkAsPermanentJobError(
				errors.Wrap(err, "vectorizer: embedding engine not available"))
		}
		return eng, nil
	}
	connURI, err := lookupExternalConnURI(ctx, execCfg, connectionName)
	if err != nil {
		return nil, errors.Wrap(err, "vectorizer: resolving external connection")
	}
	embedder, err := embedding.ResolveRemoteEmbedder(model, connURI)
	if err != nil {
		return nil, jobs.MarkAsPermanentJobError(
			errors.Wrap(err, "vectorizer: resolving remote embedder"))
	}
	return embedder, nil
}

// embedAndInsertRows embeds the given rows and writes them to the
// companion table. When isUpdate is true, existing embeddings are
// replaced via ON CONFLICT DO UPDATE; otherwise new rows are inserted
// with ON CONFLICT DO NOTHING.
//
// When loadingMode is "uri", the source column value is treated as a
// cloud storage URI. The file is fetched, its text is extracted, and
// the extracted text is embedded instead of the raw column value.
//
// When inputType is VECTORIZER_INPUT_IMAGE, the source column values
// are treated as raw image bytes and embedded using the ImageEmbedder
// interface. No chunking or template processing is applied to images.
func embedAndInsertRows(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	embedder embedding.Embedder,
	rows []tree.Datums,
	numPKCols int,
	pkColNames []string,
	sourceColumns []string,
	tmpl string,
	companionTableName tree.TableName,
	loadingMode string,
	inputType catpb.VectorizerInputType,
	isUpdate bool,
) (int64, error) {
	var embeddings [][]float32
	// For image inputs, chunk text is left empty since images are
	// embedded whole.
	chunkTexts := make([]string, len(rows))

	if inputType == catpb.VectorizerInputType_VECTORIZER_INPUT_IMAGE {
		imgEmbedder, ok := embedder.(embedding.ImageEmbedder)
		if !ok {
			return 0, errors.New(
				"vectorizer: model does not implement image embedding")
		}
		images := make([][]byte, len(rows))
		for i, row := range rows {
			images[i] = []byte(tree.MustBeDBytes(row[numPKCols]))
		}
		var err error
		embeddings, err = imgEmbedder.EmbedImageBatch(ctx, images)
		if err != nil {
			return 0, errors.Wrap(err, "vectorizer: generating image embeddings")
		}
	} else {
		texts := make([]string, len(rows))
		embeddings = make([][]float32, len(rows))
		// Track which rows have already been embedded as images so we
		// can skip them in the text batch below.
		imageHandled := make([]bool, len(rows))

		for i, row := range rows {
			if loadingMode == "uri" {
				uri := string(tree.MustBeDString(row[numPKCols]))

				// If the URI points to an image file, fetch raw bytes
				// and embed via the ImageEmbedder path instead of
				// attempting text extraction.
				if content.IsImage(uri) {
					imgEmbedder, ok := embedder.(embedding.ImageEmbedder)
					if !ok {
						return 0, errors.Newf(
							"vectorizer: model does not support image embedding for URI %s", uri)
					}
					data, err := readURIBytes(ctx, execCfg, uri)
					if err != nil {
						return 0, errors.Wrapf(err,
							"vectorizer: reading image URI for row %d", i)
					}
					vec, err := imgEmbedder.EmbedImage(ctx, data)
					if err != nil {
						return 0, errors.Wrapf(err,
							"vectorizer: embedding image from URI %s", uri)
					}
					embeddings[i] = vec
					imageHandled[i] = true
					// Leave chunkTexts[i] empty for images.
					continue
				}

				text, err := readURIContent(ctx, execCfg, uri)
				if err != nil {
					return 0, errors.Wrapf(err,
						"vectorizer: reading URI for row %d", i)
				}
				texts[i] = text
			} else {
				texts[i] = buildTextFromRow(
					row, numPKCols, sourceColumns, tmpl,
				)
			}
		}

		// Collect the text rows that still need embedding.
		var textIdxs []int
		var textInputs []string
		for i := range rows {
			if !imageHandled[i] {
				textIdxs = append(textIdxs, i)
				textInputs = append(textInputs, texts[i])
			}
		}
		if len(textInputs) > 0 {
			vecs, err := embedder.EmbedBatch(ctx, textInputs)
			if err != nil {
				return 0, errors.Wrap(err, "vectorizer: generating embeddings")
			}
			for j, idx := range textIdxs {
				embeddings[idx] = vecs[j]
			}
		}
		chunkTexts = texts
	}

	var count int64
	for i, row := range rows {
		var colNames []string
		var placeholders []string
		var args []interface{}
		argIdx := 1

		for j, pk := range pkColNames {
			colNames = append(colNames, tree.NameString("source_"+pk))
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, row[j])
			argIdx++
		}

		colNames = append(colNames,
			"chunk_seq", "chunk", "embedding", "last_embedded_at")
		placeholders = append(placeholders,
			fmt.Sprintf("$%d", argIdx),
			fmt.Sprintf("$%d", argIdx+1),
			fmt.Sprintf("$%d", argIdx+2),
			"now()",
		)
		args = append(args, 0, chunkTexts[i], vectorToSQL(embeddings[i]))

		var conflictClause string
		if isUpdate {
			uniqueCols := make([]string, len(pkColNames))
			for k, pk := range pkColNames {
				uniqueCols[k] = tree.NameString("source_" + pk)
			}
			conflictClause = fmt.Sprintf(
				"ON CONFLICT (%s, chunk_seq) DO UPDATE SET "+
					"chunk = excluded.chunk, "+
					"embedding = excluded.embedding, "+
					"last_embedded_at = now()",
				strings.Join(uniqueCols, ", "),
			)
		} else {
			conflictClause = "ON CONFLICT DO NOTHING"
		}

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) %s",
			companionTableName.String(),
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
			conflictClause,
		)

		if _, err := execCfg.InternalDB.Executor().ExecEx(
			ctx, "vectorizer-upsert-embedding", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			insertSQL, args...,
		); err != nil {
			return count, errors.Wrapf(
				err, "vectorizer: writing embedding for row %d", i,
			)
		}
		count++
	}
	return count, nil
}

// readURIContent fetches file content from a cloud storage URI and
// extracts text suitable for embedding. Supports S3, GCS, HTTP, and
// readURIBytes fetches raw file content from a cloud storage URI.
// Supports s3://, gs://, http://, and nodelocal:// URIs.
func readURIBytes(ctx context.Context, execCfg *sql.ExecutorConfig, uri string) ([]byte, error) {
	store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(
		ctx, uri, username.RootUserName(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "opening external storage")
	}
	defer store.Close()

	file, _, err := store.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return nil, errors.Wrap(err, "reading file")
	}
	data, err := ioctx.ReadAll(ctx, file)
	if err != nil {
		return nil, errors.Wrap(err, "reading file content")
	}

	if int64(len(data)) > content.MaxFileSize {
		return nil, errors.Newf(
			"file size %d bytes exceeds maximum %d bytes",
			len(data), content.MaxFileSize)
	}
	return data, nil
}

// readURIContent fetches file content from a cloud storage URI and
// extracts text. Uses readURIBytes for fetching, then applies content
// type detection and text extraction for supported formats (.txt, .md,
// .csv, .json, .pdf, etc.).
func readURIContent(ctx context.Context, execCfg *sql.ExecutorConfig, uri string) (string, error) {
	data, err := readURIBytes(ctx, execCfg, uri)
	if err != nil {
		return "", err
	}
	return content.ExtractText(data, uri)
}

// buildTextFromRow constructs the text to embed from a result row.
// The row contains [pk_cols..., source_cols...].
func buildTextFromRow(row tree.Datums, numPKCols int, sourceColumns []string, tmpl string) string {
	if tmpl != "" {
		text := tmpl
		for i, col := range sourceColumns {
			datum := row[numPKCols+i]
			text = strings.ReplaceAll(text, "$"+col, datum.String())
		}
		return text
	}
	// Default: join source column values with newlines.
	var parts []string
	for i := range sourceColumns {
		datum := row[numPKCols+i]
		parts = append(parts, datum.String())
	}
	return strings.Join(parts, "\n")
}

// vectorToSQL converts a float32 slice to a SQL VECTOR literal.
func vectorToSQL(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// lookupExternalConnURI queries system.external_connections for the
// connection URI associated with the given connection name.
func lookupExternalConnURI(
	ctx context.Context, execCfg *sql.ExecutorConfig, connectionName string,
) (string, error) {
	row, err := execCfg.InternalDB.Executor().QueryRowEx(
		ctx, "vectorizer-lookup-external-connection", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT connection_details FROM system.external_connections WHERE connection_name = $1",
		connectionName,
	)
	if err != nil {
		return "", errors.Wrap(err, "looking up external connection")
	}
	if row == nil {
		return "", errors.Newf(
			"external connection %q not found", connectionName,
		)
	}

	detailsBytes := []byte(tree.MustBeDBytes(row[0]))
	var details connectionpb.ConnectionDetails
	if err := protoutil.Unmarshal(detailsBytes, &details); err != nil {
		return "", errors.Wrap(err, "decoding external connection details")
	}

	simpleURI, ok := details.Details.(*connectionpb.ConnectionDetails_SimpleURI)
	if !ok {
		return "", errors.Newf(
			"external connection %q is not a URI-based connection",
			connectionName,
		)
	}
	return simpleURI.SimpleURI.URI, nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeVectorizer,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &vectorizerResumer{
				job: job,
				st:  settings,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
