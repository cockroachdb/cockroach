// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func initHostDir() error {
	hd := os.ExpandEnv(config.DefaultHostDir)
	return os.MkdirAll(hd, 0755)
}

func syncHosts(cloud *cloud.Cloud) error {
	hd := os.ExpandEnv(config.DefaultHostDir)

	// Write all host files. We're the only process doing this due to the file
	// lock acquired by syncAll, but other processes may be reading the host
	// files concurrently so we need to write the files atomically by writing to
	// a temporary file and renaming.
	for _, c := range cloud.Clusters {
		filename := path.Join(hd, c.Name)
		tmpFile := filename + ".tmp"

		err := func() error {
			file, err := os.Create(tmpFile)
			if err != nil {
				return errors.Wrapf(err, "problem creating file %s", filename)
			}
			defer file.Close()

			// Align columns left and separate with at least two spaces.
			tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
			if _, err := tw.Write([]byte("# user@host\tlocality\tvpcId\tcreatedAt\n")); err != nil {
				return err
			}
			for _, vm := range c.VMs {
				if _, err := tw.Write([]byte(fmt.Sprintf(
					"%s@%s\t%s\t%s\t%d\n", vm.RemoteUser, vm.PublicIP, vm.Locality(), vm.VPC, vm.CreatedAt.UnixNano()))); err != nil {
					return err
				}
			}
			if err := tw.Flush(); err != nil {
				return errors.Wrapf(err, "problem writing file %s", filename)
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if err := ensureUpdatedStateIsSane(c, filename); err != nil {
			return err
		}
		if err := os.Rename(tmpFile, filename); err != nil {
			return err
		}
	}

	return gcHostsFiles(cloud)
}

// ensureUpdatedStateIsSane checks if the cloud-read state of a cluster
// shares any IP addresses with the current serialized state of that cluster
// then the two should have the same number of nodes. A cluster might be
// destroyed and recreated with the same name and a different shape. In this
// case we expect that the old cluster and new cluster will not share any public
// IP addresses. If they do and do not have exactly the same shape, we assume
// that there is a problem. This heuristic isn't flawless, it's possible that a
// public IP addressed may be reused. If this failure mode turns out to be
// common then it may be worthwhile to add additional identifying state to
// clusters. There have been cases the state of a cluster read from the cloud
// provider seems to be missing nodes which previously were known to have
// existed.
func ensureUpdatedStateIsSane(c *cloud.Cluster, existingStatePath string) error {
	// If there is no existing state then we have nothing to check.
	// If the error is a filesystem error rather than os.ErrNotExist, we'll hit it
	// later when interacting with the filesystem so just return nil.
	if _, err := os.Stat(existingStatePath); err != nil {
		return nil
	}
	existingCluster, err := loadClusterFile(existingStatePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read existing cluster file %v, overwriting: %v\n",
			existingStatePath, err)
		return nil
	}
	var haveOverlappingVMs bool
	for i, ip := range existingCluster.VMs {
		if vm := c.VMs.FindByPublicIP(ip); vm != nil {
			// Two vms are considered to be the same if they have the same public IP
			// and creation time.
			haveOverlappingVMs = !vm.CreatedAt.IsZero() &&
				existingCluster.CreationTimes[i].Equal(vm.CreatedAt)
			if haveOverlappingVMs {
				break
			}
		}
	}
	if haveOverlappingVMs && len(existingCluster.VMs) != len(c.VMs) {
		return errors.Errorf("cluster shape for cluster %v does not match existing value, have %v expected %v", c.Name, existingCluster, c.VMs)
	}
	return nil
}

func gcHostsFiles(cloud *cloud.Cloud) error {
	hd := os.ExpandEnv(config.DefaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}
		if _, ok := cloud.Clusters[file.Name()]; ok {
			continue
		}

		filename := filepath.Join(hd, file.Name())
		if err = os.Remove(filename); err != nil {
			log.Printf("failed to remove file %s", filename)
		}
	}
	return nil
}

func newInvalidHostsLineErr(line string) error {
	return fmt.Errorf("invalid hosts line, expected <username>@<host> [locality] [vpcId] [createdAt], got %q", line)
}

func loadClusterFile(filePath string) (*install.SyncedCluster, error) {
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read %s", filePath)
	}
	lines := strings.Split(string(contents), "\n")

	c := &install.SyncedCluster{
		Name: filepath.Base(filePath),
	}
	hasCreatedAt := false
	for _, l := range lines {
		// We'll consume the fields as we go along
		fields := strings.Fields(l)
		if len(fields) == 0 {
			continue
		} else if len(fields[0]) > 0 && fields[0][0] == '#' {
			// Comment line, use this to determine if this hosts file contains a
			// created at timestamp.
			hasCreatedAt = len(fields) == 5 && fields[4] == "createdAt"
			continue
		} else if len(fields) > 4 || (!hasCreatedAt && len(fields) > 3) {
			return nil, newInvalidHostsLineErr(l)
		}

		parts := strings.Split(fields[0], "@")
		fields = fields[1:]
		var n, u string
		if len(parts) == 1 {
			u = config.OSUser.Username
			n = parts[0]
		} else if len(parts) == 2 {
			u = parts[0]
			n = parts[1]
		} else {
			return nil, newInvalidHostsLineErr(l)
		}

		var locality string
		if len(fields) > 0 {
			locality = fields[0]
			fields = fields[1:]
		}

		var vpc string
		if len(fields) > 0 {
			vpc = fields[0]
			fields = fields[1:]
		}

		var createdAt time.Time
		if hasCreatedAt {
			createdAtNanos, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				return nil, newInvalidHostsLineErr(l)
			}
			createdAt = timeutil.Unix(0, createdAtNanos)
			fields = fields[1:]
		}

		if len(fields) > 0 {
			return nil, newInvalidHostsLineErr(l)
		}

		c.VMs = append(c.VMs, n)
		c.Users = append(c.Users, u)
		c.Localities = append(c.Localities, locality)
		c.VPCs = append(c.VPCs, vpc)
		c.CreationTimes = append(c.CreationTimes, createdAt)
	}
	return c, nil
}

func loadClusters() error {
	hd := os.ExpandEnv(config.DefaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}
		if strings.HasSuffix(file.Name(), ".tmp") {
			continue
		}
		filename := filepath.Join(hd, file.Name())
		c, err := loadClusterFile(filename)
		if err != nil {
			return err
		}
		install.Clusters[file.Name()] = c
	}
	return nil
}
