// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// InitDirs TODO
func InitDirs() error {
	hd := os.ExpandEnv(config.DefaultHostDir)
	if err := os.MkdirAll(hd, 0755); err != nil {
		return err
	}
	return os.MkdirAll(os.ExpandEnv(config.DefaultDebugDir), 0755)
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
			if _, err := tw.Write([]byte("# user@host\tlocality\tvpcId\n")); err != nil {
				return err
			}
			for _, vm := range c.VMs {
				if _, err := tw.Write([]byte(fmt.Sprintf(
					"%s@%s\t%s\t%s\n", vm.RemoteUser, vm.PublicIP, vm.Locality(), vm.VPC))); err != nil {
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

		if err := os.Rename(tmpFile, filename); err != nil {
			return err
		}
	}

	return gcHostsFiles(cloud)
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
			log.Infof(context.Background(), "failed to remove file %s", filename)
		}
	}
	return nil
}

func newInvalidHostsLineErr(line string) error {
	return fmt.Errorf("invalid hosts line, expected <username>@<host> [locality] [vpcId], got %q", line)
}

// LoadClusters TODO
func LoadClusters() error {
	hd := os.ExpandEnv(config.DefaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	debugDir := os.ExpandEnv(config.DefaultDebugDir)

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}
		if strings.HasSuffix(file.Name(), ".tmp") {
			continue
		}

		filename := filepath.Join(hd, file.Name())
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "could not read %s", filename)
		}
		lines := strings.Split(string(contents), "\n")

		clusterName := file.Name()
		c := &install.SyncedCluster{
			Name:     clusterName,
			DebugDir: debugDir,
		}

		for _, l := range lines {
			// We'll consume the fields as we go along
			fields := strings.Fields(l)
			if len(fields) == 0 {
				continue
			} else if len(fields[0]) > 0 && fields[0][0] == '#' {
				// Comment line.
				continue
			} else if len(fields) > 3 {
				return newInvalidHostsLineErr(l)
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
				return newInvalidHostsLineErr(l)
			}
			// NB: it turns out we do see empty hosts here if we are concurrently
			// creating clusters and this sync is picking up a cluster that's not
			// ready yet. See:
			// https://github.com/cockroachdb/cockroach/issues/49542#issuecomment-634563130
			// if n == "" {
			// 	return newInvalidHostsLineErr(l)
			// }

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

			if len(fields) > 0 {
				return newInvalidHostsLineErr(l)
			}

			c.VMs = append(c.VMs, n)
			c.Users = append(c.Users, u)
			c.Localities = append(c.Localities, locality)
			c.VPCs = append(c.VPCs, vpc)
		}
		if len(c.VMs) == 0 {
			return errors.Errorf("found no VMs in %s", contents)
		}
		install.Clusters[clusterName] = c

		if local.IsLocal(clusterName) {
			// Add the local cluster to the local provider.
			now := timeutil.Now()
			cluster := &cloud.Cluster{
				Name:      clusterName,
				CreatedAt: now,
				Lifetime:  time.Hour,
			}
			for i := range c.VMs {
				cluster.VMs = append(cluster.VMs, vm.VM{
					Name:        "localhost",
					CreatedAt:   now,
					Lifetime:    time.Hour,
					PrivateIP:   "127.0.0.1",
					Provider:    local.ProviderName,
					ProviderID:  local.ProviderName,
					PublicIP:    "127.0.0.1",
					RemoteUser:  c.Users[i],
					VPC:         local.ProviderName,
					MachineType: local.ProviderName,
					Zone:        local.ProviderName,
				})
			}
			local.AddCluster(cluster)
		}
	}

	return nil
}

// localVMStorage implements the local.VMStorage interface.
type localVMStorage struct{}

var _ local.VMStorage = localVMStorage{}

// SaveCluster is part of the local.VMStorage interface.
func (localVMStorage) SaveCluster(cluster *cloud.Cluster) error {
	path := filepath.Join(os.ExpandEnv(config.DefaultHostDir), cluster.Name)
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "problem creating file %s", path)
	}
	defer file.Close()

	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
	if _, err := tw.Write([]byte("# user@host\tlocality\n")); err != nil {
		return err
	}
	for i := range cluster.VMs {
		if _, err := tw.Write([]byte(fmt.Sprintf(
			"%s@%s\t%s\n", cluster.VMs[i].RemoteUser, "127.0.0.1", "region=local,zone=local"))); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return errors.Wrapf(err, "problem writing file %s", path)
	}
	return nil
}

// DeleteCluster is part of the LocalVMStorage interface.
func (localVMStorage) DeleteCluster(name string) error {
	return os.Remove(filepath.Join(os.ExpandEnv(config.DefaultHostDir), name))
}
