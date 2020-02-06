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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestCgroupsGetMemory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  map[string]string
		errMsg string
		limit  int64
		warn   string
	}{
		{
			name:   "fails to find cgroup version when cgroup file is not present",
			errMsg: "failed to read memory cgroup from cgroups file:",
		},
		{
			name: "doesn't detect limit for cgroup v1 without memory controller",
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithoutMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			warn:  "no cgroup memory controller detected",
			limit: 0,
		},
		{
			name: "fails to find mount details when mountinfo is not present",
			paths: map[string]string{
				"/proc/self/cgroup": v1CgroupWithMemoryController,
			},
			errMsg: "failed to read mounts info from file:",
		},
		{
			name: "fails to find cgroup v1 version when there is no memory mount",
			paths: map[string]string{
				"/proc/self/cgroup":    v1CgroupWithMemoryController,
				"/proc/self/mountinfo": v1MountsWithoutMemController,
			},
			errMsg: "failed to detect cgroup root mount and version",
			limit:  0,
		},
		{
			name: "fetches the limit for cgroup v1",
			paths: map[string]string{
				"/proc/self/cgroup":                 v1CgroupWithMemoryController,
				"/proc/self/mountinfo":              v1MountsWithMemController,
				"/sys/fs/cgroup/memory/memory.stat": v1MemoryStat,
			},
			limit: 2936016896,
		},
		{
			name: "fails when the stat file is missing for cgroup v2",
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
			},
			errMsg: "can't read available memory from cgroup v2",
		},
		{
			name: "fails when unable to parse limit for cgroup v2",
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "unparsable\n",
			},
			errMsg: "can't parse available memory from cgroup v2 in",
		},
		{
			name: "fetches the limit for cgroup v2",
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "1073741824\n",
			},
			limit: 1073741824,
		},
		{
			name: "recognizes `max` as the limit for cgroup v2",
			paths: map[string]string{
				"/proc/self/cgroup":    v2CgroupWithMemoryController,
				"/proc/self/mountinfo": v2Mounts,
				"/sys/fs/cgroup/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope/memory.max": "max\n",
			},
			limit: 9223372036854775807,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := createFiles(t, tc.paths)
			defer func() { _ = os.RemoveAll(dir) }()

			limit, warn, err := getCgroupMem(dir)
			require.True(t, testutils.IsError(err, tc.errMsg),
				"%v %v", err, tc.errMsg)
			require.Regexp(t, tc.warn, warn)
			require.Equal(t, tc.limit, limit)
		})
	}
}

func createFiles(t *testing.T, paths map[string]string) (dir string) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	for path, data := range paths {
		path = filepath.Join(dir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, ioutil.WriteFile(path, []byte(data), 0755))
	}
	return dir
}

const (
	v1CgroupWithMemoryController = `11:blkio:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
10:devices:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:perf_event:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:cpu,cpuacct:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:pids:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:cpuset:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:memory:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v1CgroupWithoutMemoryController = `10:blkio:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
9:devices:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
8:perf_event:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
7:cpu,cpuacct:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
6:pids:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
5:cpuset:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
4:net_cls,net_prio:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
3:hugetlb:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
2:freezer:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
1:name=systemd:/kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3
`
	v2CgroupWithMemoryController = `0::/machine.slice/libpod-f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810.scope
`

	v1MountsWithMemController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
733 703 0:28 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,memory
734 703 0:29 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
736 703 0:31 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,cpu,cpuacct
737 703 0:32 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`
	v1MountsWithoutMemController = `625 367 0:71 / / rw,relatime master:85 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/DOLSFLPSKANL4GJ7XKF3OG6PKN:/var/lib/docker/overlay2/l/P7UJPLDFEUSRQ7CZILB7L4T5OP:/var/lib/docker/overlay2/l/FSKO5FFFNQ6XOSVF7T6R2DWZVZ:/var/lib/docker/overlay2/l/YNE4EZZE2GW2DIXRBUP47LB3GU:/var/lib/docker/overlay2/l/F2JNS7YWT5CU7FUXHNV5JUJWQY,upperdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/diff,workdir=/var/lib/docker/overlay2/b12d4d510f3eaf4552a749f9d4f6da182d55bfcdc75755f1972fd8ca33f51278/work
626 625 0:79 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
687 625 0:75 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
691 687 0:82 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
702 625 0:159 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
703 702 0:99 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755
705 703 0:23 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:9 - cgroup cgroup rw,xattr,release_agent=/usr/lib/systemd/systemd-cgroups-agent,name=systemd
711 703 0:25 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:10 - cgroup cgroup rw,freezer
726 703 0:26 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,hugetlb
727 703 0:27 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,net_cls,net_prio
734 703 0:29 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:14 - cgroup cgroup rw,cpuset
735 703 0:30 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,pids
736 703 0:31 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,cpu,cpuacct
737 703 0:32 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,perf_event
740 703 0:33 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,devices
742 703 0:34 /kubepods/besteffort/pod1bf924dd-3f6f-11ea-983d-0abc95f90166/c17eb535a47774285717e40bbda777ee72e81471272a5b8ebffd51fdf7f624e3 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,blkio
744 687 0:78 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
746 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/volumes/kubernetes.io~empty-dir/cockroach-env /etc/cockroach-env ro,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
760 687 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/containers/cockroachdb/3e868c1f /dev/termination-log rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
776 625 259:3 / /cockroach/cockroach-data rw,relatime - ext4 /dev/nvme2n1 rw,data=ordered
814 625 0:68 / /cockroach/cockroach-certs ro,relatime - tmpfs tmpfs rw
815 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/resolv.conf /etc/resolv.conf rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
816 625 259:1 /var/lib/docker/containers/b7d4d62b68384b4adb9b76bbe156e7a7bcd469c6d40cdd0e70f1949184260683/hostname /etc/hostname rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
817 625 259:1 /var/lib/kubelet/pods/1bf924dd-3f6f-11ea-983d-0abc95f90166/etc-hosts /etc/hosts rw,noatime - xfs /dev/nvme0n1p1 rw,attr2,inode64,noquota
818 687 0:77 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k
819 625 0:69 / /run/secrets/kubernetes.io/serviceaccount ro,relatime - tmpfs tmpfs rw
368 626 0:79 /bus /proc/bus ro,relatime - proc proc rw
375 626 0:79 /fs /proc/fs ro,relatime - proc proc rw
376 626 0:79 /irq /proc/irq ro,relatime - proc proc rw
381 626 0:79 /sys /proc/sys ro,relatime - proc proc rw
397 626 0:79 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
213 626 0:70 / /proc/acpi ro,relatime - tmpfs tmpfs ro
216 626 0:75 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
217 626 0:75 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
218 626 0:75 /null /proc/latency_stats rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
222 626 0:75 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
223 626 0:75 /null /proc/sched_debug rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755
224 702 0:101 / /sys/firmware ro,relatime - tmpfs tmpfs ro
`

	v2Mounts = `371 344 0:35 / / rw,relatime - overlay overlay rw,context="system_u:object_r:container_file_t:s0:c200,c321",lowerdir=/var/lib/containers/storage/overlay/l/SPNDOAU3AZNJMNKU3F5THCA36R,upperdir=/var/lib/containers/storage/overlay/7dcd88f815bded7b833fb5dc0f25de897250bcfa828624c0d78393689d0bc312/diff,workdir=/var/lib/containers/storage/overlay/7dcd88f815bded7b833fb5dc0f25de897250bcfa828624c0d78393689d0bc312/work
372 371 0:37 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
373 371 0:38 / /dev rw,nosuid - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=65536k,mode=755
374 371 0:39 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs rw,seclabel
375 373 0:40 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,context="system_u:object_r:container_file_t:s0:c200,c321",gid=5,mode=620,ptmxmode=666
376 373 0:36 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw,seclabel
377 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/hostname /etc/hostname rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
378 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/.containerenv /run/.containerenv rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
379 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/run/secrets /run/secrets rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
380 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/resolv.conf /etc/resolv.conf rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
381 371 0:24 /containers/storage/overlay-containers/f1c6b44c0d61f273952b8daecf154cee1be2d503b7e9184ebf7fcaf48e139810/userdata/hosts /etc/hosts rw,nosuid,nodev - tmpfs tmpfs rw,seclabel,mode=755
382 373 0:33 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=64000k
383 374 0:25 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw,seclabel
384 372 0:41 / /proc/acpi ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
385 372 0:6 /null /proc/kcore rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
386 372 0:6 /null /proc/keys rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
387 372 0:6 /null /proc/timer_list rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
388 372 0:6 /null /proc/sched_debug rw,nosuid - devtmpfs devtmpfs rw,seclabel,size=1869464k,nr_inodes=467366,mode=755
389 372 0:42 / /proc/scsi ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
390 374 0:43 / /sys/firmware ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
391 374 0:44 / /sys/fs/selinux ro,relatime - tmpfs tmpfs rw,context="system_u:object_r:container_file_t:s0:c200,c321",size=0k
392 372 0:37 /bus /proc/bus ro,relatime - proc proc rw
393 372 0:37 /fs /proc/fs ro,relatime - proc proc rw
394 372 0:37 /irq /proc/irq ro,relatime - proc proc rw
395 372 0:37 /sys /proc/sys ro,relatime - proc proc rw
396 372 0:37 /sysrq-trigger /proc/sysrq-trigger ro,relatime - proc proc rw
345 373 0:40 /0 /dev/console rw,nosuid,noexec,relatime - devpts devpts rw,context="system_u:object_r:container_file_t:s0:c200,c321",gid=5,mode=620,ptmxmode=666
`
	v1MemoryStat = `cache 784113664
rss 1703952384
rss_huge 27262976
shmem 0
mapped_file 14520320
dirty 4096
writeback 0
swap 0
pgpgin 35979039
pgpgout 35447229
pgfault 24002539
pgmajfault 3871
inactive_anon 0
active_anon 815435776
inactive_file 1363746816
active_file 308867072
unevictable 0
hierarchical_memory_limit 2936016896
hierarchical_memsw_limit 9223372036854771712
total_cache 784113664
total_rss 1703952384
total_rss_huge 27262976
total_shmem 0
total_mapped_file 14520320
total_dirty 4096
total_writeback 0
total_swap 0
total_pgpgin 35979039
total_pgpgout 35447229
total_pgfault 24002539
total_pgmajfault 3871
total_inactive_anon 0
total_active_anon 815435776
total_inactive_file 1363746816
total_active_file 308867072
total_unevictable 0
`
)
