// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwatcher

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// ClusterMetricRow represents a decoded row from the system.cluster_metrics
// table.
type ClusterMetricRow struct {
	ID          int64
	Name        string
	Labels      map[string]string
	Type        string
	Value       int64
	NodeID      int64
	LastUpdated time.Time
}

// ToMetric converts the row into a metric.Iterable using registered metadata.
// Rows without labels produce a Gauge or Counter; rows with labels produce an
// ExportedGaugeVec or ExportedCounterVec. An error is returned if no metadata
// is registered for the metric name or if the metric type is unsupported.
func (cmr ClusterMetricRow) ToMetric() (metric.Iterable, error) {
	md, labels, ok := cmmetrics.GetClusterMetricMetadata(cmr.Name)
	if !ok {
		return nil, errors.Newf("no metadata found for metric %s", cmr.Name)
	}

	if len(labels) == 0 {
		return cmr.toMetric(md)
	} else {
		return cmr.toLabeledMetric(md, labels)
	}
}

func (cmr ClusterMetricRow) toLabeledMetric(
	metadata metric.Metadata, labels []string,
) (metric.Iterable, error) {
	switch cmr.Type {
	case "gauge":
		vec := metric.NewExportedGaugeVec(metadata, labels)
		vec.Update(cmr.Labels, cmr.Value)
		return vec, nil
	case "counter":
		vec := metric.NewExportedCounterVec(metadata, labels)
		vec.Update(cmr.Labels, cmr.Value)
		return vec, nil
	default:
		return nil, errors.Newf("unknown metric type %s for exported metric %s", cmr.Type, cmr.Name)
	}
}

func (cmr ClusterMetricRow) toMetric(metadata metric.Metadata) (metric.Iterable, error) {
	switch cmr.Type {
	case "gauge":
		gauge := metric.NewGauge(metadata)
		gauge.Update(cmr.Value)
		return gauge, nil
	case "counter":
		counter := metric.NewCounter(metadata)
		counter.Update(cmr.Value)
		return counter, nil
	default:
		return nil, errors.Newf("unknown metric type %s for metric %s", cmr.Type, cmr.Name)
	}
}

// RowDecoder decodes rows from the cluster_metrics table.
type RowDecoder struct {
	codec   keys.SQLCodec
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

// MakeRowDecoder constructs a new RowDecoder for the cluster_metrics table.
func MakeRowDecoder(codec keys.SQLCodec) RowDecoder {
	columns := systemschema.ClusterMetricsTable.PublicColumns()
	return RowDecoder{
		codec:   codec,
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// DecodeRow decodes a row of the system.cluster_metrics table. If the value is
// not present, only the ID field will be populated and the tombstone bool will
// be set.
func (d *RowDecoder) DecodeRow(kv roachpb.KeyValue) (_ ClusterMetricRow, tombstone bool, _ error) {
	// Decode the primary key (id column, index 0) from the KV key.
	keyVals := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(d.codec, keyVals, nil, kv.Key); err != nil {
		return ClusterMetricRow{}, false, errors.Wrap(err, "failed to decode key")
	}
	if err := keyVals[0].EnsureDecoded(d.columns[0].GetType(), &d.alloc); err != nil {
		return ClusterMetricRow{}, false, err
	}

	var row ClusterMetricRow
	row.ID = int64(tree.MustBeDInt(keyVals[0].Datum))

	if !kv.Value.IsPresent() {
		return row, true, nil
	}

	// The rest of the columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return ClusterMetricRow{}, false, err
	}

	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return ClusterMetricRow{}, false, err
	}

	// Map datums at indices 1-6 to struct fields.
	// Index 7 is the virtual shard column (always DNull) — skip it.
	if d := datums[1]; d != tree.DNull {
		row.Name = string(tree.MustBeDString(d))
	}
	if d := datums[2]; d != tree.DNull {
		jsonStr := d.(*tree.DJSON).JSON.String()
		if err := json.Unmarshal([]byte(jsonStr), &row.Labels); err != nil {
			return ClusterMetricRow{}, false, errors.Wrap(err, "failed to decode labels JSON")
		}
	}
	if d := datums[3]; d != tree.DNull {
		row.Type = string(tree.MustBeDString(d))
	}
	if d := datums[4]; d != tree.DNull {
		row.Value = int64(tree.MustBeDInt(d))
	}
	if d := datums[5]; d != tree.DNull {
		row.NodeID = int64(tree.MustBeDInt(d))
	}
	if d := datums[6]; d != tree.DNull {
		row.LastUpdated = tree.MustBeDTimestampTZ(d).Time
	}

	return row, false, nil
}
