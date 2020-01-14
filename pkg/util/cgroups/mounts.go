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
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
From man 5 fstab:

The following is a typical example of an fstab entry:

              LABEL=t-home2   /home      ext4    defaults,auto_da_alloc
              0  2

       The first field (fs_spec).
              This field describes the block special device or remote
              filesystem to be mounted.

              For ordinary mounts, it will hold (a link to) a block special
              device node (as created by mknod(2)) for the device to be
              mounted, like `/dev/cdrom' or `/dev/sdb7'.  For NFS mounts,
              this field is <host>:<dir>, e.g., `knuth.aeb.nl:/'.  For
              filesystems with no storage, any string can be used, and will
              show up in df(1) output, for example.  Typical usage is `proc'
              for procfs; `mem', `none', or `tmpfs' for tmpfs.  Other
              special filesystems, like udev and sysfs, are typically not
              listed in fstab.

              LABEL=<label> or UUID=<uuid> may be given instead of a device
              name.  This is the recommended method, as device names are
              often a coincidence of hardware detection order, and can
              change when other disks are added or removed.  For example,
              `LABEL=Boot' or `UUID=3e6be9de-8139-11d1-9106-a43f08d823a6'.
              (Use a filesystem-specific tool like e2label(8), xfs_admin(8),
              or fatlabel(8) to set LABELs on filesystems).

              It's also possible to use PARTUUID= and PARTLABEL=. These
              partitions identifiers are supported for example for GUID
              Partition Table (GPT).

              See mount(8), blkid(8) or lsblk(8) for more details about
              device identifiers.

              Note that mount(8) uses UUIDs as strings. The string
              representation of the UUID should be based on lower case
              characters.

       The second field (fs_file).
              This field describes the mount point (target) for the
              filesystem.  For swap partitions, this field should be
              specified as `none'. If the name of the mount point contains
              spaces or tabs these can be escaped as `\040' and '\011'
              respectively.

       The third field (fs_vfstype).
              This field describes the type of the filesystem.  Linux
              supports many filesystem types: ext4, xfs, btrfs, f2fs, vfat,
              ntfs, hfsplus, tmpfs, sysfs, proc, iso9660, udf, squashfs,
              nfs, cifs, and many more.  For more details, see mount(8).

              An entry swap denotes a file or partition to be used for
              swapping, cf. swapon(8).  An entry none is useful for bind or
              move mounts.

              More than one type may be specified in a comma-separated list.

              mount(8) and umount(8) support filesystem subtypes.  The
              subtype is defined by '.subtype' suffix.  For example
              'fuse.sshfs'. It's recommended to use subtype notation rather
              than add any prefix to the first fstab field (for example
              'sshfs#example.com' is deprecated).

       The fourth field (fs_mntops).
              This field describes the mount options associated with the
              filesystem.

              It is formatted as a comma-separated list of options.  It
              contains at least the type of mount (ro or rw), plus any
              additional options appropriate to the filesystem type
              (including performance-tuning options).  For details, see
              mount(8) or swapon(8).

              Basic filesystem-independent options are:

              defaults
                     use default options: rw, suid, dev, exec, auto, nouser,
                     and async.

              noauto do not mount when "mount -a" is given (e.g., at boot
                     time)

              user   allow a user to mount

              owner  allow device owner to mount

              comment
                     or x-<name> for use by fstab-maintaining programs

              nofail do not report errors for this device if it does not
                     exist.

       The fifth field (fs_freq).
              This field is used by dump(8) to determine which filesystems
              need to be dumped.  Defaults to zero (don't dump) if not
              present.

       The sixth field (fs_passno).
              This field is used by fsck(8) to determine the order in which
              filesystem checks are done at boot time.  The root filesystem
              should be specified with a fs_passno of 1.  Other filesystems
              should have a fs_passno of 2.  Filesystems within a drive will
              be checked sequentially, but filesystems on different drives
              will be checked at the same time to utilize parallelism
              available in the hardware.  Defaults to zero (don't fsck) if
              not present.
*/
type fstabEntry struct {
	fsSpec    string
	fsFile    string
	fsVfsOpts string
	fsMntOpts string
}

func parseProcMounts(treeRoot string, f func(entry fstabEntry)) error {
	procMountsFilePath := filepath.Join(treeRoot, "proc", strconv.Itoa(os.Getpid()), "mounts")
	procMountsFile, err := os.Open(procMountsFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = procMountsFile.Close() }()
	scanner := bufio.NewScanner(procMountsFile) // scan lines
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		var e fstabEntry
		for i, field := range fields {
			switch i {
			case 0:
				e.fsSpec = field
			case 1:
				e.fsFile = field
			case 2:
				e.fsVfsOpts = field
			case 3:
				e.fsMntOpts = field
			case 4, 5:
				// Ignoring fs_freq and fs_passno for now.
			default:
				// Should never be seen but it's not obviously worth pulling in the
				// dependency on logging.
			}
		}
		f(e)
	}
	return nil
}

// getMemoryCgroupRoot reads /proc/${pid}/mounts to find the mountpoint for the
// cgroup v1 memory subsystem if it exists.
func getMemoryCgroupRoot(treeRoot string) (cgroupRoot string, err error) {
	var memorySubsystemPath string
	if err := parseProcMounts(treeRoot, func(entry fstabEntry) {
		if entry.fsSpec != cgroupV1FilesystemSpec {
			return
		}
		if !strings.Contains(entry.fsMntOpts, cgroupMemorySubsystem) {
			return
		}
		memorySubsystemPath = entry.fsFile
	}); err != nil {
		return "", err
	}
	return memorySubsystemPath, nil
}

// getUnifiedCgroupRoot reads /proc/${pid}/mounts to find the mountpoint for the
// cgroup v2 unified hierarchy if it exists.
func getUnifiedCgroupRoot(treeRoot string) (cgroupRoot string, err error) {
	var unifiedHierarchyPath string
	if err := parseProcMounts(treeRoot, func(entry fstabEntry) {
		if entry.fsSpec != cgroupV2FilesystemSpec {
			return
		}
		unifiedHierarchyPath = entry.fsFile
	}); err != nil {
		return "", err
	}
	return unifiedHierarchyPath, nil
}
