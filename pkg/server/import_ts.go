// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	yaml "gopkg.in/yaml.v2"
)

// maxBatchSize governs the maximum size of the kv batch comprising of ts data
// to be written to the DB.
const maxBatchSize = 10000

func maybeImportTS(ctx context.Context, s *topLevelServer) (returnErr error) {
	// We don't want to do startup retries as this is not meant to be run in
	// production.
	ctx = startup.WithoutChecks(ctx)

	var deferError func(error)
	{
		var defErr error
		deferError = func(err error) {
			log.Infof(ctx, "%v", err)
			defErr = errors.CombineErrors(defErr, err)
		}
		defer func() {
			if returnErr == nil {
				returnErr = defErr
			}
		}()
	}
	knobs, _ := s.cfg.TestingKnobs.Server.(*TestingKnobs)
	if knobs == nil {
		return nil
	}
	tsImport := knobs.ImportTimeseriesFile
	if tsImport == "" {
		return nil
	}

	// Suppress writing of node statuses for the local node (n1). If it wrote one,
	// and the imported data also contains n1 but with a different set of stores,
	// we'd effectively clobber the timeseries display for n1 (which relies on the
	// store statuses to map the store timeseries to the node under which they
	// fall). An alternative to this is setting a FirstStoreID and FirstNodeID that
	// is not in use in the data set to import.
	s.node.suppressNodeStatus.Store(true)

	// Disable raft log synchronization to make the server generally faster.
	logstore.DisableSyncRaftLog.Override(ctx, &s.cfg.Settings.SV, true)

	// Disable writing of new timeseries, as well as roll-ups and deletion.
	for _, stmt := range []string{
		"SET CLUSTER SETTING timeseries.storage.enabled = 'false';",
		"SET CLUSTER SETTING timeseries.storage.resolution_10s.ttl = '99999h';",
		"SET CLUSTER SETTING timeseries.storage.resolution_30m.ttl = '99999h';",
	} {
		if _, err := s.sqlServer.internalExecutor.ExecEx(
			ctx, "tsdump-cfg", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			stmt,
		); err != nil {
			return errors.Wrapf(err, "%s", stmt)
		}
	}
	if tsImport == "-" {
		// YOLO mode to look at timeseries after previous import error. This setting is used
		// to override errors seen during a prior run of tsdump. Since the timeseries data
		// has already been loaded during the previous tsdump run, we can simply return
		// here and allow the user to proceed with viewing the loaded timeseries data.
		return nil
	}
	// In practice we only allow populating time series in `start-single-node` due
	// to complexities detailed below. Additionally, we allow it only on a fresh
	// single-node single-store cluster and we also guard against join flags even
	// though there shouldn't be any.
	if !s.InitialStart() || len(s.cfg.JoinList) > 0 || len(s.cfg.Stores.Specs) != 1 {
		return errors.New("cannot import timeseries into an existing cluster or a multi-{store,node} cluster")
	}

	f, err := os.Open(tsImport)
	if err != nil {
		return err
	}
	defer f.Close()

	if knobs.ImportTimeseriesMappingFile == "" {
		knobs.ImportTimeseriesMappingFile = knobs.ImportTimeseriesFile + ".yaml"
	}

	mapBytes, err := os.ReadFile(knobs.ImportTimeseriesMappingFile)
	if err != nil {
		if oserror.IsNotExist(err) {
			err = errors.Wrapf(err, "need to specify COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE; it should point at "+
				"a YAML file that maps StoreID to NodeID. To generate from the source cluster, run the following command:\n \n"+
				"cockroach sql --url \"<(unix/sql) url>\" --format tsv -e \\\n  \"select concat(store_id::string, ': ', node_id::string)"+
				"from crdb_internal.kv_store_status\" | \\\n  grep -E '[0-9]+: [0-9]+' | tee tsdump.gob.yaml\n \n"+
				"To create from a debug.zip file, run the following command:\n \n"+
				"tail -n +2 debug/crdb_internal.kv_store_status.txt | awk '{print $2 \": \" $1}' > tsdump.gob.yaml\n \n"+
				"Then export the created file: export COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE=tsdump.gob.yaml\n \n")
		}
		return err
	}
	storeToNode := map[roachpb.StoreID]roachpb.NodeID{}
	if err := yaml.NewDecoder(bytes.NewReader(mapBytes)).Decode(&storeToNode); err != nil {
		return err
	}
	batch := &kv.Batch{}

	var batchSize int
	maybeFlush := func(force bool) error {
		if batchSize == 0 {
			// Nothing to write to DB.
			return nil
		}
		if batchSize < maxBatchSize && !force {
			return nil
		}
		err := s.db.Run(ctx, batch)
		if err != nil {
			return err
		}
		log.Infof(ctx, "imported %d ts pairs\n", batchSize)
		*batch, batchSize = kv.Batch{}, 0
		return nil
	}

	nodeIDs := map[string]struct{}{}
	storeIDs := map[string]struct{}{}
	nodeMetrics := map[string]bool{}
	storeMetrics := map[string]bool{}

	nodeMetricIdentifier := "cr.node."
	storeMetricIdentifier := "cr.store."

	dec := gob.NewDecoder(f)
	for {
		var v roachpb.KeyValue
		err := dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				if err := maybeFlush(true /* force */); err != nil {
					return err
				}
				break
			}
			return err
		}

		name, source, _, _, err := ts.DecodeDataKey(v.Key)
		if err != nil {
			deferError(err)
			continue
		}

		if strings.HasPrefix(name, nodeMetricIdentifier) {
			nodeIDs[source] = struct{}{}
			metricName := strings.Replace(name, nodeMetricIdentifier, "", 1)
			nodeMetrics[metricName] = true
		} else if strings.HasPrefix(name, storeMetricIdentifier) {
			storeIDs[source] = struct{}{}
			metricName := strings.Replace(name, storeMetricIdentifier, "", 1)
			storeMetrics[metricName] = true
		} else {
			deferError(errors.Errorf("unknown metric %s", name))
			continue
		}

		p := kvpb.NewPut(v.Key, v.Value)
		p.(*kvpb.PutRequest).Inline = true
		batch.AddRawRequest(p)
		batchSize++
		if err := maybeFlush(false /* force */); err != nil {
			return err
		}
	}

	fakeStatuses := makeFakeNodeStatuses(storeToNode, nodeMetrics, storeMetrics)
	if err := checkFakeStatuses(fakeStatuses, storeIDs); err != nil {
		// The checks are pretty strict and in particular make sure that there is at
		// least one data point for each store. Sometimes stores are down for the
		// time periods passed to tsdump or decommissioned nodes show up, etc; these
		// are important to point out but often the data set is actually OK and we
		// want to be able to view it.
		deferError(errors.Wrapf(err, "consider updating the mapping file %s or restarting the server with "+
			"COCKROACH_DEBUG_TS_IMPORT_FILE=- to ignore the error", knobs.ImportTimeseriesMappingFile))
	}

	// Write the statuses.
	for _, status := range fakeStatuses {
		key := keys.NodeStatusKey(status.Desc.NodeID)
		if err := s.db.PutInline(ctx, key, &status); err != nil {
			return err
		}
	}

	return nil
}

func makeFakeNodeStatuses(
	storeToNode map[roachpb.StoreID]roachpb.NodeID,
	nodeMetrics map[string]bool,
	storeMetrics map[string]bool,
) []statuspb.NodeStatus {

	var sl []statuspb.NodeStatus
	nodeToStore := map[roachpb.NodeID][]roachpb.StoreID{}
	for sid, nid := range storeToNode {
		nodeToStore[nid] = append(nodeToStore[nid], sid)
	}

	for nodeID, storeIDs := range nodeToStore {
		sort.Slice(storeIDs, func(i, j int) bool {
			return storeIDs[i] < storeIDs[j]
		})
		nodeStatus := statuspb.NodeStatus{
			Desc: roachpb.NodeDescriptor{
				NodeID: nodeID,
			},
		}

		// Populate nodeStatus.Metrics and storeStatus.Metrics
		// with the metric names found in the dump.
		//
		// A metric name prefixed with the `storeMetricIdentifier` should be
		// included in both `nodeStatus.Metrics` and `storeStatus.Metrics`.
		//
		// A metric name prefixed with the `nodeMetricIdentifier` should only be
		// included in `nodeStatus.Metrics`.
		//
		// The value of the metric is not consequential here.

		if nodeMetrics != nil || storeMetrics != nil {
			nodeStatus.Metrics = map[string]float64{}
		}

		for metricName := range nodeMetrics {
			nodeStatus.Metrics[metricName] = 0.0
		}

		for metricName := range storeMetrics {
			nodeStatus.Metrics[metricName] = 0.0
		}

		for _, storeID := range storeIDs {
			storeStatus := statuspb.StoreStatus{
				Desc: roachpb.StoreDescriptor{
					Node:    nodeStatus.Desc, // don't want cycles here
					StoreID: storeID,
				},
			}

			if storeMetrics != nil {
				storeStatus.Metrics = map[string]float64{}
			}

			for metricName := range storeMetrics {
				storeStatus.Metrics[metricName] = 0.0
			}

			nodeStatus.StoreStatuses = append(nodeStatus.StoreStatuses, storeStatus)
		}

		sl = append(sl, nodeStatus)
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].Desc.NodeID < sl[j].Desc.NodeID
	})
	return sl
}

func checkFakeStatuses(fakeStatuses []statuspb.NodeStatus, storeIDs map[string]struct{}) error {
	for _, status := range fakeStatuses {
		for _, ss := range status.StoreStatuses {
			storeID := ss.Desc.StoreID
			strID := fmt.Sprint(storeID)
			if _, ok := storeIDs[strID]; !ok {
				// This is likely an mistake and where it isn't (for example since store
				// is long gone and hasn't supplied metrics in a long time) the user can
				// react by removing the assignment from the yaml file and trying again.
				return errors.Errorf(
					"s%d supplied in input mapping, but no timeseries found for it",
					ss.Desc.StoreID,
				)
			}

			delete(storeIDs, strID)
		}
	}
	if len(storeIDs) > 0 {
		return errors.Errorf(
			"need to map the remaining stores %v to nodes", storeIDs)
	}
	return nil
}
