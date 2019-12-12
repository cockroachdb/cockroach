// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cgroups

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestCgroupsGetMemory(t *testing.T) {
	pid := os.Getpid()
	mapping := func(s string) string {
		if s == "pid" {
			return strconv.Itoa(pid)
		}
		return ""
	}
	createFiles := func(t *testing.T, paths map[string]string) (dir string) {
		dir, err := ioutil.TempDir("", "")
		require.NoError(t, err)

		for path, data := range paths {
			path = filepath.Join(dir, os.Expand(path, mapping))
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
			require.NoError(t, ioutil.WriteFile(path, []byte(data), 0755))
		}
		return dir
	}
	const v1Mounts = `sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
udev /dev devtmpfs rw,nosuid,relatime,size=7685540k,nr_inodes=1921385,mode=755 0 0
devpts /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0
tmpfs /run tmpfs rw,nosuid,noexec,relatime,size=1539340k,mode=755 0 0
/dev/sda1 / ext4 rw,relatime,data=ordered 0 0
securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0
tmpfs /dev/shm tmpfs rw,nosuid,nodev 0 0
tmpfs /run/lock tmpfs rw,nosuid,nodev,noexec,relatime,size=5120k 0 0
tmpfs /sys/fs/cgroup tmpfs ro,nosuid,nodev,noexec,mode=755 0 0
cgroup /sys/fs/cgroup/systemd cgroup rw,nosuid,nodev,noexec,relatime,xattr,release_agent=/lib/systemd/systemd-cgroups-agent,name=systemd 0 0
pstore /sys/fs/pstore pstore rw,nosuid,nodev,noexec,relatime 0 0
cgroup /sys/fs/cgroup/freezer cgroup rw,nosuid,nodev,noexec,relatime,freezer 0 0
cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpu,cpuacct 0 0
cgroup /sys/fs/cgroup/net_cls,net_prio cgroup rw,nosuid,nodev,noexec,relatime,net_cls,net_prio 0 0
cgroup /sys/fs/cgroup/devices cgroup rw,nosuid,nodev,noexec,relatime,devices 0 0
cgroup /sys/fs/cgroup/memory cgroup rw,nosuid,nodev,noexec,relatime,memory 0 0
cgroup /sys/fs/cgroup/cpuset cgroup rw,nosuid,nodev,noexec,relatime,cpuset 0 0
cgroup /sys/fs/cgroup/rdma cgroup rw,nosuid,nodev,noexec,relatime,rdma 0 0
cgroup /sys/fs/cgroup/pids cgroup rw,nosuid,nodev,noexec,relatime,pids 0 0
cgroup /sys/fs/cgroup/perf_event cgroup rw,nosuid,nodev,noexec,relatime,perf_event 0 0
cgroup /sys/fs/cgroup/hugetlb cgroup rw,nosuid,nodev,noexec,relatime,hugetlb 0 0
cgroup /sys/fs/cgroup/blkio cgroup rw,nosuid,nodev,noexec,relatime,blkio 0 0
systemd-1 /proc/sys/fs/binfmt_misc autofs rw,relatime,fd=27,pgrp=1,timeout=0,minproto=5,maxproto=5,direct,pipe_ino=14670 0 0
debugfs /sys/kernel/debug debugfs rw,relatime 0 0
mqueue /dev/mqueue mqueue rw,relatime 0 0
hugetlbfs /dev/hugepages hugetlbfs rw,relatime,pagesize=2M 0 0
fusectl /sys/fs/fuse/connections fusectl rw,relatime 0 0
configfs /sys/kernel/config configfs rw,relatime 0 0
lxcfs /var/lib/lxcfs fuse.lxcfs rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other 0 0
/dev/nvme0n1 /mnt/data1 ext4 rw,relatime,discard,nobarrier,data=ordered 0 0
tmpfs /run/user/1000 tmpfs rw,nosuid,nodev,relatime,size=1539340k,mode=700,uid=1000,gid=1000 0 0`
	const v2Mounts = `/dev/root / ext4 rw,relatime 0 0
devtmpfs /dev devtmpfs rw,relatime,size=7691232k,nr_inodes=1922808,mode=755 0 0
sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0
tmpfs /dev/shm tmpfs rw,nosuid,nodev 0 0
devpts /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0
tmpfs /run tmpfs rw,nosuid,nodev,size=1539080k,mode=755 0 0
tmpfs /run/lock tmpfs rw,nosuid,nodev,noexec,relatime,size=5120k 0 0
cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate 0 0
pstore /sys/fs/pstore pstore rw,nosuid,nodev,noexec,relatime 0 0
bpf /sys/fs/bpf bpf rw,nosuid,nodev,noexec,relatime,mode=700 0 0
debugfs /sys/kernel/debug debugfs rw,relatime 0 0
hugetlbfs /dev/hugepages hugetlbfs rw,relatime,pagesize=2M 0 0
mqueue /dev/mqueue mqueue rw,relatime 0 0
fusectl /sys/fs/fuse/connections fusectl rw,relatime 0 0
configfs /sys/kernel/config configfs rw,relatime 0 0
/dev/loop0 /snap/google-cloud-sdk/101 squashfs ro,nodev,relatime 0 0
/dev/sda15 /boot/efi vfat rw,relatime,fmask=0022,dmask=0022,codepage=437,iocharset=iso8859-1,shortname=mixed,errors=remount-ro 0 0                                                                                
/dev/loop1 /snap/core/7713 squashfs ro,nodev,relatime 0 0
/dev/loop2 /snap/lxd/12100 squashfs ro,nodev,relatime 0 0
/dev/nvme0n1 /mnt/data1 ext4 rw,relatime,discard,nobarrier 0 0
tmpfs /run/snapd/ns tmpfs rw,nosuid,nodev,size=1539080k,mode=755 0 0
nsfs /run/snapd/ns/lxd.mnt nsfs rw 0 0
tmpfs /run/user/1000 tmpfs rw,nosuid,nodev,relatime,size=1539076k,mode=700,uid=1000,gid=1001 0 0`
	for _, tc := range []struct {
		name       string
		paths      map[string]string
		limit      int64
		errPat     string
		warningPat string
	}{
		{
			name: "simple,root",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/
11:pids:/
10:perf_event:/
9:hugetlb:/
8:blkio:/
7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/
4:rdma:/
3:cpuset:/
2:devices:/
1:name=systemd:/
0::/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes": "12345",
			},
			limit: 12345,
		},
		{
			name: "intermediate cgroup contains limit",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/
11:pids:/
10:perf_event:/
9:hugetlb:/
8:blkio:/
7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/system.slice/sshd.service
4:rdma:/
3:cpuset:/
2:devices:/
1:name=systemd:/
0::/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes":                           "123456",
				"/sys/fs/cgroup/memory/system.slice/sshd.service/memory.limit_in_bytes": "1234567",
				"/sys/fs/cgroup/memory/system.slice/memory.limit_in_bytes":              "12345",
				"/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes":                      "1234",
			},
			limit: 12345,
		},
		{
			name: "malformed proc cgroup file with value in child",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/

7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/asdf
4:rdma:/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes":      "123456",
				"/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes": "12345",
			},
			limit: 12345,
		},
		{
			name: "malformed limit file gets ignored",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/

7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/asdf
4:rdma:/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes":      "12345a sdasdfasd12341",
				"/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes": "123456",
			},
			limit: 123456,
		},
		{
			name: "all malformed files leads to an error",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/

7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/asdf
4:rdma:/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes":      "12345a sdasdfasd12341",
				"/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes": "qwer",
			},
			errPat: "can't read available memory from cgroup at .*/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes: strconv.ParseInt: parsing \"qwer\"",
		},
		{
			name: "one malformed files leads to a warning",
			paths: map[string]string{
				"/proc/${pid}/mounts": v1Mounts,
				"/proc/${pid}/cgroup": `12:freezer:/

7:cpu,cpuacct:/
6:net_cls,net_prio:/
5:memory:/asdf
4:rdma:/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes":      "12345",
				"/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes": "qwer",
			},
			limit:      12345,
			warningPat: "can't read available memory from cgroup at .*/sys/fs/cgroup/memory/asdf/memory.limit_in_bytes: strconv.ParseInt: parsing \"qwer\"",
		},
		{
			name: "no memory subsystem in /proc/.../cgroup",
			paths: map[string]string{
				"/proc/${pid}/cgroup": `12:freezer:/
7:cpu,cpuacct:/
6:net_cls,net_prio:/
4:rdma:/
`,
				"/sys/fs/cgroup/memory/memory.limit_in_bytes": "12345",
			},
			errPat: "failed to find memory cgroup",
		},
		{
			name: "v2",
			paths: map[string]string{
				"/proc/${pid}/mounts": v2Mounts,
				"/proc/${pid}/cgroup": `0::/user.slice/user-1000.slice/session-4.scope
`,
				"/sys/fs/cgroup/user.slice/user-1000.slice/session-4.scope/memory.max": `max`,
				"/sys/fs/cgroup/user.slice/user-1000.slice/memory.max":                 `99999744`,
				"/sys/fs/cgroup/user.slice/memory.max":                                 `max`,
			},
			limit: 99999744,
		},
		{
			name: "v2 no memory controllers",
			paths: map[string]string{
				"/proc/${pid}/mounts": v2Mounts,
				"/proc/${pid}/cgroup": `0::/user.slice/user-1000.slice/session-4.scope
`,
				"/sys/fs/cgroup/user.slice/user-1000.slice/session-4.scope/cgroup.controllers": `cpu`,
			},
			errPat: "failed to find cgroup memory limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := createFiles(t, tc.paths)
			defer func() { _ = os.RemoveAll(dir) }()
			limit, warning, err := getCgroupMem(dir)
			require.True(t, testutils.IsError(err, tc.errPat),
				"%v %v", err, tc.errPat)
			require.Regexp(t, tc.warningPat, warning)
			require.Equal(t, tc.limit, limit)
		})
	}
}
