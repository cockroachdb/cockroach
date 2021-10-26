// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm/local"
	"github.com/cockroachdb/errors"
)

// The code in this file deals with storing cluster metadata in the
// config.ClustersDir.
//
// This directory is used as a local cache storing metadata from all known
// clusters. It is also used as the "source of truth" for the local cluster.
//
// Each cluster corresponds to a json file in this directory.

func initDirs() error {
	cd := os.ExpandEnv(config.ClustersDir)
	if err := os.MkdirAll(cd, 0755); err != nil {
		return err
	}
	return os.MkdirAll(os.ExpandEnv(config.DefaultDebugDir), 0755)
}

// saveCluster creates (or overwrites) the file in config.ClusterDir storing the
// given metadata.
func saveCluster(c *cloud.Cluster) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		return err
	}

	filename := clusterFilename(c.Name)

	// Other roachprod processes might be accessing the cluster files at the same
	// time, so we need to write the file atomically by writing to a temporary
	// file and renaming.
	tmpFilename := filename + ".tmp"
	if err := os.WriteFile(tmpFilename, b.Bytes(), 0660); err != nil {
		return err
	}
	return os.Rename(tmpFilename, filename)
}

// loadCluster reads the file in config.ClustersDir with the metadata for the
// given cluster name.
func loadCluster(name string) (*cloud.Cluster, error) {
	filename := clusterFilename(name)
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	c := &cloud.Cluster{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	if c.Name != name {
		return nil, errors.Errorf("name mismatch (%s vs %s)", name, c.Name)
	}
	return c, nil
}

// loadClusters reads all the metadata in config.ClustersDir and populates
// install.Clusters.
func loadClusters() error {
	clusterNames, err := listClustersInCache()
	if err != nil {
		return err
	}

	debugDir := os.ExpandEnv(config.DefaultDebugDir)

	for _, name := range clusterNames {
		c, err := loadCluster(name)
		if err != nil {
			return errors.Wrapf(err, "could not load info for cluster %s", name)
		}

		if len(c.VMs) == 0 {
			return errors.Errorf("found no VMs in %s", clusterFilename(name))
		}

		sc := &install.SyncedCluster{
			Cluster:  *c,
			DebugDir: debugDir,
		}

		for _, vm := range c.VMs {
			sc.Localities = append(sc.Localities, vm.Locality())
		}

		install.Clusters[sc.Name] = sc

		if config.IsLocalClusterName(c.Name) {
			// Add the local cluster to the local provider.
			local.AddCluster(c)
		}
	}

	return nil
}

// syncClustersCache synchronizes the ClustersDir with the available clusters
// (across all providers, including any local cluster).
//
// A file in ClustersDir is created for each cluster; other files are removed.
func syncClustersCache(cloud *cloud.Cloud) error {
	// Write all cluster files.
	for _, c := range cloud.Clusters {
		if err := saveCluster(c); err != nil {
			return err
		}
	}

	// Remove any other files.
	clusterNames, err := listClustersInCache()
	if err != nil {
		return err
	}
	for _, name := range clusterNames {
		if _, ok := cloud.Clusters[name]; !ok {
			filename := clusterFilename(name)
			if err := os.Remove(filename); err != nil {
				log.Printf("failed to remove file %s", filename)
			}
		}
	}

	return nil
}

// clusterFilename returns the filename in config.ClusterDir corresponding to a
// cluster name.
func clusterFilename(name string) string {
	cd := os.ExpandEnv(config.ClustersDir)
	return path.Join(cd, name+".json")
}

// listClustersInCache returns the list of cluster names that have corresponding
// files in config.ClusterDir.
func listClustersInCache() ([]string, error) {
	var result []string
	cd := os.ExpandEnv(config.ClustersDir)
	files, err := os.ReadDir(cd)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		result = append(result, strings.TrimSuffix(file.Name(), ".json"))
	}
	return result, nil
}

// localVMStorage implements the local.VMStorage interface.
type localVMStorage struct{}

var _ local.VMStorage = localVMStorage{}

// SaveCluster is part of the local.VMStorage interface.
func (localVMStorage) SaveCluster(cluster *cloud.Cluster) error {
	return saveCluster(cluster)
}

// DeleteCluster is part of the local.VMStorage interface.
func (localVMStorage) DeleteCluster(name string) error {
	return os.Remove(clusterFilename(name))
}
