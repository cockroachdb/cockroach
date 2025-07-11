// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var cockroachIOController = filepath.Join("/sys/fs/cgroup/system.slice", install.VirtualClusterLabel(install.SystemInterfaceName, 0)+".service", "io.max")

const CgroupsDiskStallName = "cgroup-disk-stall"

type CGroupDiskStaller struct {
	GenericFailure
	// Used in conjunction with the `Cycle` option to manage the
	// async script.
	waitCh      <-chan error
	cancelCycle func()
}

func MakeCgroupDiskStaller(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	genericFailure, err := makeGenericFailure(clusterName, l, clusterOpts, CgroupsDiskStallName)
	if err != nil {
		return nil, err
	}
	return &CGroupDiskStaller{GenericFailure: *genericFailure}, nil
}

func registerCgroupDiskStall(r *FailureRegistry) {
	r.add(CgroupsDiskStallName, DiskStallArgs{}, MakeCgroupDiskStaller)
}

type DiskStallArgs struct {
	StallLogs   bool
	StallReads  bool
	StallWrites bool
	// If true, allow the failure mode to restart nodes as needed. E.g. dmsetup requires
	// the cockroach process to not be running to properly setup. If RestartNodes is true,
	// then the failure mode will restart the cluster for the user.
	RestartNodes bool
	// Throughput is the bytes per second to throttle reads/writes to. If unset, will
	// stall reads/write completely. Supported only for cgroup disk staller, dmsetup
	// only supports fully stalling reads/writes.
	Throughput int
	Nodes      install.Nodes
	// If true, the failure mode will repeatedly stall and unstall the disk
	// until recovered. The duration of the stall and unstall cycles is 5 seconds
	// by default but can be configured with the CycleStallDuration and
	// CycleUnstallDuration args.
	//
	// N.B. Because this is run asynchronously, any script errors will only be
	// logged instead of returned.
	Cycle                bool
	CycleStallDuration   time.Duration
	CycleUnstallDuration time.Duration
}

func (s *CGroupDiskStaller) Description() string {
	return CgroupsDiskStallName
}

func (s *CGroupDiskStaller) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	diskStallArgs := args.(DiskStallArgs)

	// Cgroup throttles a specific disk device, however our logs directory
	// is usually mounted on a different device than our cockroach data. To
	// stall both logs and the cockroach process, they must both be mounted
	// on the same device. To do so, we create a new logs directory in our
	// stalled device, e.g. {store-dir}/logs, and create a symlink from logs
	// to that directory.
	//
	// If the cluster is already running, we want to make sure we don't lose
	// any existing logs. We first move our existing logs to a temporary
	// directory, before copying them into the new symlinked directory.
	if diskStallArgs.StallLogs {
		// N.B. Because multiple FS operations aren't atomic, we must temporarily
		// stop the cluster before moving the logs directory.
		if diskStallArgs.RestartNodes {
			if err := s.StopCluster(ctx, l, roachprod.DefaultStopOpts()); err != nil {
				return err
			}
		}

		tmpLogsDir := fmt.Sprintf("tmp-disk-stall-%d", timeutil.Now().Unix())
		createSymlinkCmd := fmt.Sprintf(`
if [ ! -L logs ]; then
    if [ -e logs ]; then
				echo "moving existing logs to tmp directory %[1]s"
				mv logs %[1]s
    fi
    mkdir -p {store-dir}/logs
		echo "creating symlink logs -> {store-dir}/logs";
    ln -s {store-dir}/logs logs
		if [ -e %[1]s ]; then
				echo "copying tmp directory %[1]s to logs";
				cp -va %[1]s/* logs/
		fi
else
		echo "symlink already exists, not creating";
fi
`, tmpLogsDir)
		if err := s.Run(ctx, l, diskStallArgs.Nodes, createSymlinkCmd); err != nil {
			return err
		}
		if diskStallArgs.RestartNodes {
			if err := s.StartCluster(ctx, l); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *CGroupDiskStaller) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	defer s.CloseConnections()
	diskStallArgs := args.(DiskStallArgs)
	nodes := diskStallArgs.Nodes

	if args.(DiskStallArgs).StallLogs {
		// Cleanup our symlinked logs. Similar to Setup(), we must first stop the cluster
		// to stop the cockroach process from concurrently writing to the logs directory.
		if err := s.Run(ctx, l, nodes, "unlink logs"); err != nil {
			return err
		}
		if diskStallArgs.RestartNodes {
			if err := s.StopCluster(ctx, l, roachprod.DefaultStopOpts()); err != nil {
				return err
			}
		}
		if err := s.Run(ctx, l, nodes, "cp -r {store-dir}/logs logs"); err != nil {
			return err
		}
		if diskStallArgs.RestartNodes {
			return s.StartCluster(ctx, l)
		}
	}
	return nil
}

func getStallTypes(diskStallArgs DiskStallArgs) ([]bandwidthType, error) {
	var stallTypes []bandwidthType
	if diskStallArgs.StallWrites {
		stallTypes = []bandwidthType{writeBandwidth}
	} else if diskStallArgs.StallLogs {
		return nil, errors.New("stalling logs is not supported without stalling writes")
	}
	if diskStallArgs.StallReads {
		stallTypes = append(stallTypes, readBandwidth)
	}
	if len(stallTypes) == 0 {
		return nil, errors.New("at least one of reads or writes must be stalled")
	}
	return stallTypes, nil
}

func (s *CGroupDiskStaller) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	diskStallArgs := args.(DiskStallArgs)
	stallTypes, err := getStallTypes(diskStallArgs)
	if err != nil {
		return err
	}

	// N.B. Although the cgroupsv2 documentation states that "limits are in the range [0, max]",
	// attempting to set a bytesPerSecond=0 results in a `Numerical result out of range` error
	// from the io.max cgroupv2 API. Upon inspection of the blk-throttle implementation, we can
	// see an explicit `if (!val)` error check disallowing 0 values.
	//
	// Similarly, attempting to set a bytesPerSecond=1 results in an `Invalid argument` error
	// due to an additional check that `val > 1`. Interestingly, this appears to be an Ubunutu
	// 22.04+ addition, as older distributions and the upstream cgroup implementation do not
	// have this check.
	//
	// This additional check appears to protect against the io hanging when allowing bursts
	// of io, i.e. allowing io limits to gradually accumulate even if the soft limit is too low
	// to serve the system's request. Said burst allowance is calculated roughly as:
	// `adj_limit = limit + (limit >> 1) * adj_limit`. When the limit is 1, we can see
	// the adjusted limit will never increase, potentially blocking io requests indefinitely.
	// While this is exactly what  we want, it's not the intended use case and is invalid.
	bytesPerSecond := 2
	if diskStallArgs.Throughput == 1 {
		return errors.New("cgroups v2 requires a io throughput of at least 2 bytes per second")
	} else if diskStallArgs.Throughput > 1 {
		bytesPerSecond = diskStallArgs.Throughput
	}

	nodes := diskStallArgs.Nodes

	// Shuffle the order of read and write stall initiation.
	rand.Shuffle(len(stallTypes), func(i, j int) {
		stallTypes[i], stallTypes[j] = stallTypes[j], stallTypes[i]
	})

	defer func() {
		// Log the cgroup bandwidth limits for debugging purposes.
		err = s.Run(ctx, l, nodes, "cat", cockroachIOController)
		if err != nil {
			l.Printf("failed to log cgroup bandwidth limits: %v", err)
		}
	}()

	stallCmd, err := s.setThroughputCmd(ctx, l, stallTypes, throughput{limited: true, bytesPerSecond: fmt.Sprintf("%d", bytesPerSecond)}, cockroachIOController)
	if err != nil {
		return err
	}

	if diskStallArgs.Cycle {
		unstallCmd, err := s.setThroughputCmd(ctx, l, stallTypes, throughput{limited: false}, cockroachIOController)
		if err != nil {
			return err
		}
		s.waitCh, s.cancelCycle = s.stallCycle(ctx, l, diskStallArgs, stallCmd, unstallCmd)
		return nil
	}

	l.Printf("stalling disk I/O on nodes %d", nodes)
	return s.Run(ctx, l, nodes, stallCmd)
}

func (s *CGroupDiskStaller) Recover(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	diskStallArgs := args.(DiskStallArgs)
	stallTypes, err := getStallTypes(diskStallArgs)
	if err != nil {
		return err
	}

	nodes := diskStallArgs.Nodes

	cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", install.VirtualClusterLabel(install.SystemInterfaceName, 0)+".service", "io.max")

	// If we are running an async disk stall cycle script, cancel the script and
	// block until it returns.
	if diskStallArgs.Cycle {
		l.Printf("stopping disk stall cycle on nodes %d", nodes)
		s.cancelCycle()
		select {
		case err = <-s.waitCh:
			// We expect this to finish with a context canceled error, but log
			// it anyway in case the script exited early.
			l.Printf("disk stall cycle script finished with error: %v", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	l.Printf("unstalling disk I/O on nodes %d", nodes)
	// N.B. cgroups v2 relies on systemd running, however if our disk stall
	// was injected for too long, the cockroach process will detect a disk stall
	// and exit. This deletes the cockroach service and there is no need to
	// unlimit anything.
	cmd, err := s.setThroughputCmd(ctx, l, stallTypes, throughput{limited: false}, cockroachIOController)
	if err != nil {
		return err
	}
	err = s.Run(ctx, l, diskStallArgs.Nodes, cmd)

	// If we aren't restarting nodes, then return the error we encountered attempting to
	// unstall the disk.
	if !diskStallArgs.RestartNodes {
		return err
	}

	// If we are restarting nodes, then we assume the failure above is because
	// the disk stall was injected too long, and it was an expected death. Restart
	// any dead nodes.
	return forEachNode(diskStallArgs.Nodes, func(n install.Nodes) error {
		err = s.Run(ctx, l, n, "cat", cockroachIOController)
		if err != nil && diskStallArgs.RestartNodes {
			l.Printf("failed to log cgroup bandwidth limits, assuming n%d exited and restarting: %v", n, err)
			return s.StartNodes(ctx, l, n)
		}
		return nil
	})
}

func (s *CGroupDiskStaller) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	if args.(DiskStallArgs).Cycle {
		l.Printf("Stall cycle is enabled, skipping WaitForFailureToPropagate")
		return nil
	}

	diskStallArgs := args.(DiskStallArgs)
	if diskStallArgs.StallWrites {
		// If writes are stalled, we expect the disk stall detection to kick in
		// and kill the node.
		return forEachNode(diskStallArgs.Nodes, func(n install.Nodes) error {
			return s.WaitForSQLUnavailable(ctx, l, n, 3*time.Minute)
		})
	}
	return nil
}

func (s *CGroupDiskStaller) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	diskStallArgs := args.(DiskStallArgs)
	nodes := diskStallArgs.Nodes
	return s.WaitForRestartedNodesToStabilize(ctx, l, nodes, 20*time.Minute)
}

type throughput struct {
	limited        bool
	bytesPerSecond string
}

type bandwidthType int8

const (
	readBandwidth bandwidthType = iota
	writeBandwidth
)

func (rw bandwidthType) cgroupV2BandwidthProp() string {
	switch rw {
	case readBandwidth:
		return "rbps"
	case writeBandwidth:
		return "wbps"
	default:
		panic("unreachable")
	}
}

func (s *CGroupDiskStaller) setThroughputCmd(
	ctx context.Context,
	l *logger.Logger,
	readOrWrite []bandwidthType,
	bw throughput,
	cockroachIOController string,
) (string, error) {
	maj, min, err := s.DiskDeviceMajorMinor(ctx, l)
	if err != nil {
		return "", err
	}

	var limits []string
	for _, rw := range readOrWrite {
		bytesPerSecondStr := "max"
		if bw.limited {
			bytesPerSecondStr = bw.bytesPerSecond
		}
		limits = append(limits, fmt.Sprintf("%s=%s", rw.cgroupV2BandwidthProp(), bytesPerSecondStr))
	}
	l.Printf("setting cgroup bandwith limits:\n%v", limits)

	return fmt.Sprintf("sudo /bin/bash -c 'echo %d:%d %s > %s'",
		maj,
		min,
		strings.Join(limits, " "),
		cockroachIOController,
	), nil
}

// GetReadWriteBytes parses the io.stat file to get the number of bytes read and written.
// TODO(darryl): switch to using a lightweight exporter instead: https://github.com/cockroachdb/cockroach/issues/144052
func (s *CGroupDiskStaller) GetReadWriteBytes(
	ctx context.Context, l *logger.Logger, node install.Nodes,
) (int, int, error) {
	maj, min, err := s.DiskDeviceMajorMinor(ctx, l)
	if err != nil {
		return 0, 0, err
	}
	// Check the number of bytes read and written to disk.
	res, err := s.RunWithDetails(
		ctx, l, node,
		fmt.Sprintf(`grep -E '%d:%d' /sys/fs/cgroup/system.slice/io.stat |`, maj, min),
		`grep -oE 'rbytes=[0-9]+|wbytes=[0-9]+' |`,
		`awk -F= '{printf "%s ", $2} END {print ""}'`,
	)
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(res.Stdout)
	if len(fields) != 2 {
		return 0, 0, errors.Errorf("expected 2 fields, got %d: %s", len(fields), res.Stdout)
	}

	readBytes, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, err
	}
	writeBytes, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, err
	}

	return readBytes, writeBytes, nil
}

const (
	DmsetupDiskStallName = "dmsetup-disk-stall"
	dmsetupStallCmd      = "sudo dmsetup suspend --noflush --nolockfs data1"
	dmsetupUnstallCmd    = "sudo dmsetup resume data1"
)

type DmsetupDiskStaller struct {
	GenericFailure
	// Used in conjunction with the `Cycle` option to manage the
	// async script.
	waitCh      <-chan error
	cancelCycle func()
}

func MakeDmsetupDiskStaller(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	genericFailure, err := makeGenericFailure(clusterName, l, clusterOpts, DmsetupDiskStallName)
	if err != nil {
		return nil, err
	}
	return &DmsetupDiskStaller{GenericFailure: *genericFailure}, nil
}

func registerDmsetupDiskStall(r *FailureRegistry) {
	r.add(DmsetupDiskStallName, DiskStallArgs{}, MakeDmsetupDiskStaller)
}

func (s *DmsetupDiskStaller) Description() string {
	return "dmsetup disk staller"
}

func (s *DmsetupDiskStaller) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	diskStallArgs := args.(DiskStallArgs)
	var err error

	// Disabling journaling requires the cockroach process to not have been started yet.
	if diskStallArgs.RestartNodes {
		// Use the default stop opts, if the user wants more control, they should manage
		// the cluster restart themselves.
		stopOpts := roachprod.DefaultStopOpts()
		if err = s.StopCluster(ctx, l, stopOpts); err != nil {
			return err
		}
	}

	dev, err := s.DiskDeviceName(ctx, l)
	if err != nil {
		return err
	}

	// snapd will run "snapd auto-import /dev/dm-0" via udev triggers when
	// /dev/dm-0 is created. This possibly interferes with the dmsetup create
	// reload, so uninstall snapd.
	if err = s.Run(ctx, l, s.c.Nodes, `sudo apt-get purge -y snapd`); err != nil {
		return err
	}
	if err = s.Run(ctx, l, s.c.Nodes, `sudo umount -f /mnt/data1 || true`); err != nil {
		return err
	}
	if err = s.Run(ctx, l, s.c.Nodes, `sudo dmsetup remove_all`); err != nil {
		return err
	}
	// See https://github.com/cockroachdb/cockroach/issues/129619#issuecomment-2316147244.
	if err = s.Run(ctx, l, s.c.Nodes, `sudo tune2fs -O ^has_journal `+dev); err != nil {
		return errors.WithHintf(err, "disabling journaling fails if the cluster has been started")
	}
	if err = s.Run(ctx, l, s.c.Nodes, `echo "0 $(sudo blockdev --getsz `+dev+`) linear `+dev+` 0" | `+
		`sudo dmsetup create data1`); err != nil {
		return err
	}
	// This has occasionally been seen to fail with "Device or resource busy",
	// with no clear explanation. Try to find out who it is.
	if err = s.Run(ctx, l, s.c.Nodes, "sudo bash -c 'ps aux; dmsetup status; mount; lsof'"); err != nil {
		return err
	}

	if err = s.Run(ctx, l, s.c.Nodes, `sudo mount /dev/mapper/data1 /mnt/data1`); err != nil {
		return err
	}

	if diskStallArgs.RestartNodes {
		if err = s.StartCluster(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func (s *DmsetupDiskStaller) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	nodes := args.(DiskStallArgs).Nodes
	l.Printf("stalling disk I/O on nodes %d", nodes)
	if args.(DiskStallArgs).Cycle {
		s.waitCh, s.cancelCycle = s.stallCycle(ctx, l, args.(DiskStallArgs), dmsetupStallCmd, dmsetupUnstallCmd)
		return nil
	}

	return s.Run(ctx, l, nodes, dmsetupStallCmd)
}

func (s *DmsetupDiskStaller) Recover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	diskStallArgs := args.(DiskStallArgs)
	nodes := diskStallArgs.Nodes

	// If we are running an async disk stall cycle script, cancel the script and
	// block until it returns.
	if diskStallArgs.Cycle {
		l.Printf("stopping disk stall cycle on nodes %d", nodes)
		s.cancelCycle()
		select {
		case err := <-s.waitCh:
			// We expect this to finish with a context canceled error, but log
			// it anyway in case the script exited early.
			l.Printf("disk stall cycle script finished with error: %v", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	l.Printf("unstalling disk I/O on nodes %d", nodes)
	if err := s.Run(ctx, l, nodes, `sudo dmsetup resume data1`); err != nil {
		return err
	}

	if diskStallArgs.RestartNodes {
		// If the disk stall was injected for long enough that the cockroach process
		// detected it and shut down the node, then restart it.
		return forEachNode(nodes, func(n install.Nodes) error {
			if err := s.PingNode(ctx, l, n); err != nil {
				l.Printf("failed to connect to n%d, assuming node exited and restarting: %v", n, err)
				return s.StartNodes(ctx, l, n)
			}
			return nil
		})
	}

	return nil
}

func (s *DmsetupDiskStaller) Cleanup(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	defer s.CloseConnections()

	diskStallArgs := args.(DiskStallArgs)
	if diskStallArgs.RestartNodes {
		stopOpts := roachprod.DefaultStopOpts()
		if err := s.StopCluster(ctx, l, stopOpts); err != nil {
			return err
		}
	}

	dev, err := s.DiskDeviceName(ctx, l)
	if err != nil {
		return err
	}

	if err := s.Run(ctx, l, s.c.Nodes, `sudo dmsetup resume data1`); err != nil {
		return err
	}
	if err := s.Run(ctx, l, s.c.Nodes, `sudo umount /mnt/data1`); err != nil {
		return err
	}
	if err := s.Run(ctx, l, s.c.Nodes, `sudo dmsetup remove data1`); err != nil {
		return err
	}
	if err := s.Run(ctx, l, s.c.Nodes, `sudo tune2fs -O has_journal `+dev); err != nil {
		return err
	}
	if err := s.Run(ctx, l, s.c.Nodes, `sudo mount /mnt/data1`); err != nil {
		return err
	}
	// Reinstall snapd.
	if err := s.Run(ctx, l, s.c.Nodes, `sudo apt-get install -y snapd`); err != nil {
		return err
	}

	// When we unmounted the disk in setup, the cgroups controllers may have been removed, re-add them.
	if err := s.Run(ctx, l, s.c.Nodes, "sudo", "/bin/bash", "-c",
		`'echo "+cpuset +cpu +io +memory +pids" > /sys/fs/cgroup/cgroup.subtree_control'`); err != nil {
		return err
	}
	if err := s.Run(ctx, l, s.c.Nodes, "sudo", "/bin/bash", "-c",
		`'echo "+cpuset +cpu +io +memory +pids" > /sys/fs/cgroup/system.slice/cgroup.subtree_control'`); err != nil {
		return err
	}

	if diskStallArgs.RestartNodes {
		if err := s.StartCluster(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func (s *DmsetupDiskStaller) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	if args.(DiskStallArgs).Cycle {
		l.Printf("Stall cycle is enabled, skipping WaitForFailureToPropagate")
		return nil
	}
	nodes := args.(DiskStallArgs).Nodes
	// Since writes are stalled for dmsetup, we expect the disk stall detection to kick in
	// and eventually kill the node.
	return forEachNode(nodes, func(n install.Nodes) error {
		return s.WaitForSQLUnavailable(ctx, l, n, 3*time.Minute)
	})
}

func (s *DmsetupDiskStaller) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	diskStallArgs := args.(DiskStallArgs)
	nodes := diskStallArgs.Nodes
	return s.WaitForRestartedNodesToStabilize(ctx, l, nodes, 20*time.Minute)
}

// stallCycle asynchronously runs a script in the background that repeatedly stalls and
// unstalls the disk. It returns an error channel that returns any errors encountered
// while running the script, and a cancel function to stop the script.
func (f *GenericFailure) stallCycle(
	ctx context.Context, l *logger.Logger, args DiskStallArgs, stallCmd, unstallCmd string,
) (<-chan error, func()) {
	// By default, induce stalls in 5 second intervals.
	stallSleep, unstallSleep := float64(5*time.Second.Milliseconds()), float64(5*time.Second.Milliseconds())
	if args.CycleStallDuration > 0 {
		stallSleep = float64(args.CycleStallDuration.Milliseconds())
	}
	if args.CycleUnstallDuration > 0 {
		unstallSleep = float64(args.CycleUnstallDuration.Milliseconds())
	}

	// `time sleep` accepts fractional seconds, but not milliseconds.
	stallSleep /= 1000
	unstallSleep /= 1000

	cycleCmd := fmt.Sprintf(
		`
stall() {
  %[1]s
  echo "$(date '+%%Y-%%m-%%d %%H:%%M:%%S.%%3N'): disk stalled for %[2]f seconds"
}

unstall() {
  %[3]s
  echo "$(date '+%%Y-%%m-%%d %%H:%%M:%%S.%%3N'): disk unstalled for %[4]f seconds"
}

# Always attempt to unstall on script exit. We don't rely on Recover()
# as unstalling a disk stall is time sensitive and waiting too long
# may cause the node to fatal.
trap 'unstall' EXIT INT TERM HUP

while true; do
  stall
	time sleep %[2]f
  unstall
	time sleep %[4]f
done
`, stallCmd, stallSleep, unstallCmd, unstallSleep)

	l.Printf("starting disk stall cycle on nodes %d", args.Nodes)

	return runAsync(ctx, l, func(ctx context.Context) error {
		return f.Run(ctx, l, args.Nodes, cycleCmd)
	})
}
