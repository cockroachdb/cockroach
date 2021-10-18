// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"bytes"
	"fmt"
	"sort"
)

// Clusters memoizes cluster info for install operations
var Clusters = map[string]*SyncedCluster{}

var installCmds = map[string]string{
	"cassandra": `
echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | \
	sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list;
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -;
sudo apt-get update;
sudo apt-get install -y cassandra;
sudo service cassandra stop;
`,

	"charybdefs": `
  thrift_dir="/opt/thrift"

  if [ ! -f "/usr/bin/thrift" ]; then
	sudo apt-get update;
	sudo apt-get install -qy automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config python-setuptools libglib2.0-dev python2 python-six

    sudo mkdir -p "${thrift_dir}"
    sudo chmod 777 "${thrift_dir}"
    cd "${thrift_dir}"
    curl "https://downloads.apache.org/thrift/0.13.0/thrift-0.13.0.tar.gz" | sudo tar xvz --strip-components 1
    sudo ./configure --prefix=/usr
    sudo make -j$(nproc)
    sudo make install
    (cd "${thrift_dir}/lib/py" && sudo python2 setup.py install)
  fi

  charybde_dir="/opt/charybdefs"
  nemesis_path="${charybde_dir}/charybdefs-nemesis"

  if [ ! -f "${nemesis_path}" ]; then
    sudo apt-get install -qy build-essential cmake libfuse-dev fuse
    sudo rm -rf "${charybde_dir}" "${nemesis_path}" /usr/local/bin/charybdefs{,-nemesis}
    sudo mkdir -p "${charybde_dir}"
    sudo chmod 777 "${charybde_dir}"
    # TODO(bilal): Change URL back to scylladb/charybdefs once https://github.com/scylladb/charybdefs/pull/28 is merged.
    git clone --depth 1 "https://github.com/itsbilal/charybdefs.git" "${charybde_dir}"

    cd "${charybde_dir}"
    thrift -r --gen cpp server.thrift
    cmake CMakeLists.txt
    make -j$(nproc)

    sudo modprobe fuse
    sudo ln -s "${charybde_dir}/charybdefs" /usr/local/bin/charybdefs
    cat > "${nemesis_path}" <<EOF
#!/bin/bash
cd /opt/charybdefs/cookbook
./recipes "\$@"
EOF
    chmod +x "${nemesis_path}"
	sudo ln -s "${nemesis_path}" /usr/local/bin/charybdefs-nemesis
fi
`,

	"confluent": `
sudo apt-get update;
sudo apt-get install -y default-jdk-headless;
curl https://packages.confluent.io/archive/5.0/confluent-oss-5.0.0-2.11.tar.gz | sudo tar -C /usr/local -xz;
sudo ln -s /usr/local/confluent-5.0.0 /usr/local/confluent;
`,

	"docker": `
sudo apt-get update;
sudo apt-get install  -y \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common;
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -;
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable";

sudo apt-get update;
sudo apt-get install  -y docker-ce;
`,

	"gcc": `
sudo apt-get update;
sudo apt-get install -y gcc;
`,

	// graphviz and rlwrap are useful for pprof
	"go": `
sudo apt-get update;
sudo apt-get install -y graphviz rlwrap;

curl https://dl.google.com/go/go1.12.linux-amd64.tar.gz | sudo tar -C /usr/local -xz;
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh > /dev/null;
sudo chmod +x /etc/profile.d/go.sh;
`,

	"haproxy": `
sudo apt-get update;
sudo apt-get install -y haproxy;
`,

	"ntp": `
sudo apt-get update;
sudo apt-get install -y \
  ntp \
  ntpdate;
`,

	"sysbench": `
curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | sudo bash;
sudo apt-get update;
sudo apt-get install -y sysbench;
`,

	"tools": `
sudo apt-get update;
sudo apt-get install -y \
  fio \
  iftop \
  iotop \
  sysstat \
  linux-tools-common \
  linux-tools-4.10.0-35-generic \
  linux-cloud-tools-4.10.0-35-generic;
`,

	"zfs": `
sudo apt-get update;
sudo apt-get install -y \
  zfsutils-linux;
`,
}

// SortedCmds TODO(peter): document
func SortedCmds() []string {
	cmds := make([]string, 0, len(installCmds))
	for cmd := range installCmds {
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)
	return cmds
}

// Install TODO(peter): document
func Install(c *SyncedCluster, args []string) error {
	do := func(title, cmd string) error {
		var buf bytes.Buffer
		err := c.Run(&buf, &buf, c.Nodes, "installing "+title, cmd)
		if err != nil {
			fmt.Print(buf.String())
		}
		return err
	}

	for _, arg := range args {
		cmd, ok := installCmds[arg]
		if !ok {
			return fmt.Errorf("unknown tool %q", arg)
		}

		// Ensure that we early exit if any of the shell statements fail.
		cmd = "set -exuo pipefail;" + cmd
		if err := do(arg, cmd); err != nil {
			return err
		}
	}
	return nil
}
