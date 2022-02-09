// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	yaml "gopkg.in/yaml.v2"
)

func maybeImportTS(ctx context.Context, s *Server) error {
	knobs, _ := s.cfg.TestingKnobs.Server.(*TestingKnobs)
	if knobs == nil {
		return nil
	}
	tsImport := knobs.ImportTimeseriesFile
	if tsImport == "" {
		return nil
	}

	// In practice we only allow populating time series in `start-single-node` due
	// to complexities detailed below. Additionally, we allow it only on a fresh
	// single-node single-store cluster and we also guard against join flags even
	// though there shouldn't be any.
	if !s.InitialStart() || len(s.cfg.JoinList) > 0 || len(s.cfg.Stores.Specs) != 1 {
		return errors.New("cannot import timeseries into an existing cluster or a multi-{store,node} cluster")
	}

	// Also do a best effort at disabling the timeseries of the local node to cause
	// confusion.
	ts.TimeseriesStorageEnabled.Override(ctx, &s.ClusterSettings().SV, false)

	// Suppress writing of node statuses for the local node (n1). If it wrote one,
	// and the imported data also contains n1 but with a different set of stores,
	// we'd effectively clobber the timeseries display for n1 (which relies on the
	// store statuses to map the store timeseries to the node under which they
	// fall). An alternative to this is setting a FirstStoreID and FirstNodeID that
	// is not in use in the data set to import.
	s.node.suppressNodeStatus.Set(true)

	f, err := os.Open(tsImport)
	if err != nil {
		return err
	}
	defer f.Close()

	b := &kv.Batch{}
	var n int
	maybeFlush := func(force bool) error {
		if n == 0 {
			return nil
		}
		if n < 100 && !force {
			return nil
		}
		err := s.db.Run(ctx, b)
		if err != nil {
			return err
		}
		log.Infof(ctx, "imported %d ts pairs\n", n)
		*b, n = kv.Batch{}, 0
		return nil
	}

	nodeIDs := map[string]struct{}{}
	storeIDs := map[string]struct{}{}
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
			return err
		}
		if strings.HasPrefix(name, "cr.node.") {
			nodeIDs[source] = struct{}{}
		} else if strings.HasPrefix(name, "cr.store.") {
			storeIDs[source] = struct{}{}
		} else {
			return errors.Errorf("unknown metric %s", name)
		}

		p := roachpb.NewPut(v.Key, v.Value)
		p.(*roachpb.PutRequest).Inline = true
		b.AddRawRequest(p)
		n++
		if err := maybeFlush(false /* force */); err != nil {
			return err
		}
	}

	if knobs.ImportTimeseriesMappingFile == "" {
		return errors.Errorf("need to specify COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE; it should point at " +
			"a YAML file that maps StoreID to NodeID. To generate from the source cluster, run the following command:\n \n" +
			"cockroach sql --url \"<(unix/sql) url>\" --format tsv -e \\\n  \"select concat(store_id::string, ': ', node_id::string)" +
			"from crdb_internal.kv_store_status\" | \\\n  grep -E '[0-9]+: [0-9]+' | tee tsdump.gob.yaml\n \n" +
			"To create from a debug.zip file, run the following command:\n \n" +
			"tail -n +2 debug/crdb_internal.kv_store_status.txt | awk '{print $2 \": \" $1}' > tsdump.gob.yaml\n \n" +
			"Then export the created file: export COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE=tsdump.gob.yaml\n \n")
	}
	mapBytes, err := ioutil.ReadFile(knobs.ImportTimeseriesMappingFile)
	if err != nil {
		return err
	}
	storeToNode := map[roachpb.StoreID]roachpb.NodeID{}
	if err := yaml.NewDecoder(bytes.NewReader(mapBytes)).Decode(&storeToNode); err != nil {
		return err
	}

	fakeStatuses, err := makeFakeNodeStatuses(storeToNode)
	if err != nil {
		return err
	}
	if err := checkFakeStatuses(fakeStatuses, storeIDs); err != nil {
		return errors.Wrapf(err, "please provide an updated mapping file %s", knobs.ImportTimeseriesMappingFile)
	}

	// All checks passed, write the statuses.
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
) ([]statuspb.NodeStatus, error) {
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
		for _, storeID := range storeIDs {
			nodeStatus.StoreStatuses = append(nodeStatus.StoreStatuses, statuspb.StoreStatus{Desc: roachpb.StoreDescriptor{
				Node:    nodeStatus.Desc, // don't want cycles here
				StoreID: storeID,
			}})
		}

		sl = append(sl, nodeStatus)
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].Desc.NodeID < sl[j].Desc.NodeID
	})
	return sl, nil
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
