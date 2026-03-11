// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// tsdump2duck converts a CockroachDB tsdump.gob file into SQL that can be
// piped into DuckDB for interactive exploration.
//
// Usage:
//
//	tsdump2duck <tsdump.gob> [tsdump.gob.yaml] | duckdb mydb.duckdb
package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

const batchSize = 1000

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <tsdump.gob> [tsdump.gob.yaml]\n", os.Args[0])
		os.Exit(1)
	}
	gobPath := os.Args[1]

	f, err := os.Open(gobPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec := gob.NewDecoder(f)

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
		dec = gob.NewDecoder(f)
	}

	// Try loading YAML sidecar if no embedded mapping (or if explicitly provided).
	if len(os.Args) == 3 {
		m, err := loadYAML(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		storeToNode = m
		fmt.Fprintf(os.Stderr, "loaded %d store-to-node entries from %s\n", len(storeToNode), os.Args[2])
	} else if storeToNode == nil {
		// Try the default sidecar path.
		yamlPath := gobPath + ".yaml"
		if m, err := loadYAML(yamlPath); err == nil {
			storeToNode = m
			fmt.Fprintf(os.Stderr, "loaded %d store-to-node entries from %s\n", len(storeToNode), yamlPath)
		}
	}

	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer out.Flush()

	fmt.Fprintln(out, `CREATE TABLE timeseries (
    name    VARCHAR NOT NULL,
    source  VARCHAR NOT NULL,
    ts      TIMESTAMP NOT NULL,
    value   DOUBLE NOT NULL
);`)

	fmt.Fprintln(out, `CREATE TABLE store_node_map (
    store_id VARCHAR NOT NULL,
    node_id  VARCHAR NOT NULL
);`)

	fmt.Fprintln(out, `CREATE TABLE node_region_map (
    node_id VARCHAR NOT NULL,
    region  VARCHAR NOT NULL
);`)

	// Decode and emit timeseries data.
	var batch []string
	var totalRows int
	flush := func() {
		if len(batch) == 0 {
			return
		}
		fmt.Fprintln(out, "INSERT INTO timeseries VALUES")
		fmt.Fprintln(out, strings.Join(batch, ",\n")+";")
		batch = batch[:0]
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

		for _, dp := range data.Datapoints {
			t := time.Unix(0, dp.TimestampNanos).UTC()
			batch = append(batch, fmt.Sprintf("  ('%s', '%s', '%s', %g)",
				escapeSQLString(data.Name),
				escapeSQLString(data.Source),
				t.Format("2006-01-02 15:04:05"),
				dp.Value))
			totalRows++
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
	flush()

	// Emit store-to-node mapping.
	if len(storeToNode) > 0 {
		fmt.Fprintln(out, "INSERT INTO store_node_map VALUES")
		var rows []string
		for storeID, nodeID := range storeToNode {
			rows = append(rows, fmt.Sprintf("  ('%s', '%s')", escapeSQLString(storeID), escapeSQLString(nodeID)))
		}
		fmt.Fprintln(out, strings.Join(rows, ",\n")+";")
	}

	// Emit node-to-region mapping.
	if len(nodeToRegion) > 0 {
		fmt.Fprintln(out, "INSERT INTO node_region_map VALUES")
		var rows []string
		for nodeID, region := range nodeToRegion {
			rows = append(rows, fmt.Sprintf("  ('%s', '%s')", escapeSQLString(nodeID), escapeSQLString(region)))
		}
		fmt.Fprintln(out, strings.Join(rows, ",\n")+";")
	}

	fmt.Fprintln(out, "CREATE INDEX idx_timeseries ON timeseries (name, source, ts);")

	fmt.Fprintf(os.Stderr, "wrote %d datapoints\n", totalRows)
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
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
