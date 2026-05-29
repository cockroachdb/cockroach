// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// tsdump2duck converts a CockroachDB tsdump.gob file into a Parquet file that
// any tool with a Parquet reader (DuckDB, pandas, polars, DataFusion, etc.)
// can query directly.
//
// Usage:
//
//	tsdump2duck -o tsdump.parquet <tsdump.gob> [tsdump.gob.yaml]
//	duckdb -c "SELECT name, count(*) FROM 'tsdump.parquet' GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
//
// If embedded or sidecar store-to-node and node-to-region mappings are
// present, they are written to <out>.store_node_map.parquet and
// <out>.node_region_map.parquet next to the main file.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// rowGroupSize is the number of rows buffered before a row group is flushed.
// 256k rows × 4 columns is a reasonable balance between memory and read perf.
const rowGroupSize = 256 * 1024

// countingReader wraps an io.Reader and atomically counts bytes read.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// isTerminal reports whether f refers to a character device (terminal/PTY).
// Avoids pulling in golang.org/x/term.
func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}

func humanBytes(n int64) string {
	const u = 1024
	if n < u {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(u), 0
	for v := n / u; v >= u; v /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func main() {
	outPath := flag.String("o", "", "output Parquet path (required)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -o <out.parquet> <tsdump.gob> [tsdump.gob.yaml]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if *outPath == "" || flag.NArg() < 1 || flag.NArg() > 2 {
		flag.Usage()
		os.Exit(1)
	}
	gobPath := flag.Arg(0)
	yamlArg := ""
	if flag.NArg() == 2 {
		yamlArg = flag.Arg(1)
	}

	f, err := os.Open(gobPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gobSize := int64(-1)
	if st, err := f.Stat(); err == nil {
		gobSize = st.Size()
	}

	gobReader := &countingReader{r: f}
	dec := gob.NewDecoder(gobReader)

	// Try reading embedded metadata.
	var storeToNode map[string]string
	var nodeToRegion map[string]string
	md, mdErr := tsdumpmeta.Read(dec)
	if mdErr == nil {
		storeToNode = md.StoreToNodeMap
		nodeToRegion = md.NodeToRegionMap
		fmt.Fprintf(os.Stderr, "read embedded metadata: version=%s, %d store-to-node entries, %d node-to-region entries\n",
			md.Version, len(storeToNode), len(nodeToRegion))
	} else {
		// No embedded metadata; restart from beginning.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			log.Fatal(err)
		}
		gobReader.n.Store(0)
		dec = gob.NewDecoder(gobReader)
	}

	// Try loading YAML sidecar if no embedded mapping (or if explicitly provided).
	if yamlArg != "" {
		m, err := loadYAML(yamlArg)
		if err != nil {
			log.Fatal(err)
		}
		storeToNode = m
		fmt.Fprintf(os.Stderr, "loaded %d store-to-node entries from %s\n", len(storeToNode), yamlArg)
	} else if storeToNode == nil {
		yamlPath := gobPath + ".yaml"
		if m, err := loadYAML(yamlPath); err == nil {
			storeToNode = m
			fmt.Fprintf(os.Stderr, "loaded %d store-to-node entries from %s\n", len(storeToNode), yamlPath)
		}
	}

	// Open output Parquet file with Snappy compression and the timeseries
	// schema (name, source, ts, value).
	tsSchema, err := buildTimeseriesSchema()
	if err != nil {
		log.Fatal(err)
	}
	outFile, err := os.Create(*outPath)
	if err != nil {
		log.Fatal(err)
	}
	props := parquet.NewWriterProperties(
		parquet.WithCreatedBy("tsdump2duck"),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	pw := file.NewParquetWriter(outFile, tsSchema.Root(), file.WithWriterProps(props))

	// Periodic progress reporter. Silent unless stderr is a terminal, so we
	// don't pollute pipes, CI logs, or any test output.
	var totalRows atomic.Int64
	progressDone := make(chan struct{})
	if isTerminal(os.Stderr) {
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			start := time.Now()
			for {
				select {
				case <-progressDone:
					return
				case now := <-t.C:
					gobBytes := gobReader.n.Load()
					rows := totalRows.Load()
					elapsed := now.Sub(start).Seconds()
					if gobSize > 0 {
						fmt.Fprintf(os.Stderr,
							"progress: gob read %s/%s (%.1f%%), %d rows emitted, %.1fs elapsed\n",
							humanBytes(gobBytes), humanBytes(gobSize),
							100*float64(gobBytes)/float64(gobSize), rows, elapsed)
					} else {
						fmt.Fprintf(os.Stderr,
							"progress: gob read %s, %d rows emitted, %.1fs elapsed\n",
							humanBytes(gobBytes), rows, elapsed)
					}
				}
			}
		}()
	}
	defer close(progressDone)

	// Buffered columnar batches. We flush a row group every rowGroupSize rows.
	names := make([]parquet.ByteArray, 0, rowGroupSize)
	sources := make([]parquet.ByteArray, 0, rowGroupSize)
	tsMicros := make([]int64, 0, rowGroupSize)
	values := make([]float64, 0, rowGroupSize)

	flush := func() {
		if len(names) == 0 {
			return
		}
		rg := pw.AppendBufferedRowGroup()
		writeBatch := func(colIdx int, fn func(file.ColumnChunkWriter) error) {
			cw, err := rg.Column(colIdx)
			if err != nil {
				log.Fatal(err)
			}
			if err := fn(cw); err != nil {
				log.Fatal(err)
			}
		}
		writeBatch(0, func(cw file.ColumnChunkWriter) error {
			_, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(names, nil, nil)
			return err
		})
		writeBatch(1, func(cw file.ColumnChunkWriter) error {
			_, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(sources, nil, nil)
			return err
		})
		writeBatch(2, func(cw file.ColumnChunkWriter) error {
			_, err := cw.(*file.Int64ColumnChunkWriter).WriteBatch(tsMicros, nil, nil)
			return err
		})
		writeBatch(3, func(cw file.ColumnChunkWriter) error {
			_, err := cw.(*file.Float64ColumnChunkWriter).WriteBatch(values, nil, nil)
			return err
		})
		if err := rg.Close(); err != nil {
			log.Fatal(err)
		}
		names = names[:0]
		sources = sources[:0]
		tsMicros = tsMicros[:0]
		values = values[:0]
	}

	for {
		var kv roachpb.KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		var data *tspb.TimeSeriesData
		dumper := ts.DefaultDumper{Send: func(d *tspb.TimeSeriesData) error {
			data = d
			return nil
		}}
		if err := dumper.Dump(&kv); err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping entry: %v\n", err)
			continue
		}

		nameBA := parquet.ByteArray(data.Name)
		sourceBA := parquet.ByteArray(data.Source)
		for _, dp := range data.Datapoints {
			names = append(names, nameBA)
			sources = append(sources, sourceBA)
			tsMicros = append(tsMicros, dp.TimestampNanos/1000)
			values = append(values, dp.Value)
			totalRows.Add(1)
			if len(names) >= rowGroupSize {
				flush()
			}
		}
	}
	flush()

	if err := pw.Close(); err != nil {
		log.Fatal(err)
	}

	// Write small sidecar Parquet files for the store→node and node→region
	// maps when present.
	if len(storeToNode) > 0 {
		path := sidecarPath(*outPath, "store_node_map")
		if err := writeStringMapParquet(path, "store_id", "node_id", storeToNode); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %d store-to-node entries to %s\n", len(storeToNode), path)
	}
	if len(nodeToRegion) > 0 {
		path := sidecarPath(*outPath, "node_region_map")
		if err := writeStringMapParquet(path, "node_id", "region", nodeToRegion); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %d node-to-region entries to %s\n", len(nodeToRegion), path)
	}

	st, _ := outFile.Stat()
	if st != nil {
		fmt.Fprintf(os.Stderr, "wrote %d datapoints to %s (%s)\n",
			totalRows.Load(), *outPath, humanBytes(st.Size()))
	} else {
		fmt.Fprintf(os.Stderr, "wrote %d datapoints to %s\n", totalRows.Load(), *outPath)
	}
}

const (
	defaultSchemaFieldID = int32(-1)
	defaultTypeLength    = -1
)

// buildTimeseriesSchema returns the Parquet schema:
//
//	name    STRING
//	source  STRING
//	ts      TIMESTAMP(MICROS, UTC)
//	value   DOUBLE
func buildTimeseriesSchema() (*schema.Schema, error) {
	stringType := schema.StringLogicalType{}
	tsType := schema.NewTimestampLogicalType(true /* utc */, schema.TimeUnitMicros)

	nameNode, err := schema.NewPrimitiveNodeLogical("name",
		parquet.Repetitions.Required, stringType,
		parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
	if err != nil {
		return nil, err
	}
	sourceNode, err := schema.NewPrimitiveNodeLogical("source",
		parquet.Repetitions.Required, stringType,
		parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
	if err != nil {
		return nil, err
	}
	tsNode, err := schema.NewPrimitiveNodeLogical("ts",
		parquet.Repetitions.Required, tsType,
		parquet.Types.Int64, defaultTypeLength, defaultSchemaFieldID)
	if err != nil {
		return nil, err
	}
	valueNode := schema.NewFloat64Node("value", parquet.Repetitions.Required, defaultSchemaFieldID)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		[]schema.Node{nameNode, sourceNode, tsNode, valueNode}, defaultSchemaFieldID)
	if err != nil {
		return nil, err
	}
	return schema.NewSchema(root), nil
}

// writeStringMapParquet writes a 2-column STRING/STRING parquet file from a
// map. Used for the small store→node and node→region sidecar tables.
func writeStringMapParquet(path, col1, col2 string, m map[string]string) error {
	stringType := schema.StringLogicalType{}
	c1, err := schema.NewPrimitiveNodeLogical(col1,
		parquet.Repetitions.Required, stringType,
		parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
	if err != nil {
		return err
	}
	c2, err := schema.NewPrimitiveNodeLogical(col2,
		parquet.Repetitions.Required, stringType,
		parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
	if err != nil {
		return err
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		[]schema.Node{c1, c2}, defaultSchemaFieldID)
	if err != nil {
		return err
	}
	sch := schema.NewSchema(root)

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	props := parquet.NewWriterProperties(
		parquet.WithCreatedBy("tsdump2duck"),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	pw := file.NewParquetWriter(out, sch.Root(), file.WithWriterProps(props))

	keys := make([]parquet.ByteArray, 0, len(m))
	vals := make([]parquet.ByteArray, 0, len(m))
	for k, v := range m {
		keys = append(keys, parquet.ByteArray(k))
		vals = append(vals, parquet.ByteArray(v))
	}
	rg := pw.AppendBufferedRowGroup()
	cw0, err := rg.Column(0)
	if err != nil {
		return err
	}
	if _, err := cw0.(*file.ByteArrayColumnChunkWriter).WriteBatch(keys, nil, nil); err != nil {
		return err
	}
	cw1, err := rg.Column(1)
	if err != nil {
		return err
	}
	if _, err := cw1.(*file.ByteArrayColumnChunkWriter).WriteBatch(vals, nil, nil); err != nil {
		return err
	}
	if err := rg.Close(); err != nil {
		return err
	}
	return pw.Close()
}

// sidecarPath returns "<dir>/<base>.<suffix>.parquet" for outPath.
// E.g. sidecarPath("foo.parquet", "store_node_map") = "foo.store_node_map.parquet".
func sidecarPath(outPath, suffix string) string {
	dir, base := filepath.Split(outPath)
	base = strings.TrimSuffix(base, ".parquet")
	return filepath.Join(dir, base+"."+suffix+".parquet")
}

func loadYAML(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return m, nil
}
