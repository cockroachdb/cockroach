// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachprod

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// The code in this file deals with storing cluster metadata in the
// config.ClustersDir.
//
// This directory is used as a local cache storing metadata from all known
// clusters. It is also used as the "source of truth" for the local cluster.
//
// Each cluster corresponds to a json file in this directory.

// InitDirs initializes the directories for storing cluster metadata and debug
// logs.
func InitDirs() error {
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
	// file and renaming. We store the temporary file in the same directory so
	// that it can always be renamed.
	tmpFile, err := os.CreateTemp(os.ExpandEnv(config.ClustersDir), c.Name)
	if err != nil {
		return err
	}

	_, err = tmpFile.Write(b.Bytes())
	err = errors.CombineErrors(err, tmpFile.Sync())
	err = errors.CombineErrors(err, tmpFile.Close())
	if err == nil {
		err = os.Rename(tmpFile.Name(), filename)
	}
	if err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}
	return nil
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

// shouldIgnoreCluster returns true if the cluster references a project that is
// not active. This is relevant if we have a cluster that was cached when
// another project was in use.
func shouldIgnoreCluster(c *cloud.Cluster) bool {
	for i := range c.VMs {
		provider, ok := vm.Providers[c.VMs[i].Provider]
		if !ok || !provider.ProjectActive(c.VMs[i].Project) {
			return true
		}
	}
	return false
}

// LoadClusters reads the cached cluster metadata from config.ClustersDir and
// populates install.Clusters.
func LoadClusters() error {
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
		if shouldIgnoreCluster(c) {
			continue
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
			// This cluster may no longer exist, or it may involve projects that are
			// not active in the current invocation.
			c, err := loadCluster(name)
			if err != nil {
				return err
			}
			if !shouldIgnoreCluster(c) {
				filename := clusterFilename(name)
				if err := os.Remove(filename); err != nil {
					log.Infof(context.Background(), "failed to remove file %s", filename)
				}
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
