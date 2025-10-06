// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/elastic/gosigar"
)

func computeStoreProperties(ctx context.Context, cfg engineConfig) roachpb.StoreProperties {
	props := roachpb.StoreProperties{
		Dir:       cfg.env.Dir,
		ReadOnly:  cfg.env.IsReadOnly(),
		Encrypted: cfg.env.Encryption != nil,
	}
	if cfg.opts.WALFailover != nil {
		props.WalFailoverPath = new(string)
		*props.WalFailoverPath = cfg.opts.WALFailover.Secondary.Dirname
	}

	// In-memory store?
	if cfg.env.Dir == "" {
		return props
	}

	fsprops := getFileSystemProperties(ctx, cfg.env.Dir)
	props.FileStoreProperties = &fsprops
	return props
}

func getFileSystemProperties(ctx context.Context, dir string) roachpb.FileStoreProperties {
	fsprops := roachpb.FileStoreProperties{
		Path: dir,
	}

	// Find which filesystem supports the store.

	absPath, err := filepath.Abs(dir)
	if err != nil {
		log.Warningf(ctx, "cannot compute absolute file path for %q: %v", dir, err)
		return fsprops
	}

	// Alas, only BSD reliably populates "fs" in os.StatFs(),
	// so we must find the filesystem manually.
	//
	// Note that scanning the list of mounts is also
	// what linux' df(1) command does.
	//
	var fslist gosigar.FileSystemList
	if err := fslist.Get(); err != nil {
		log.Warningf(ctx, "cannot retrieve filesystem list: %v", err)
		return fsprops
	}

	var fsInfo *gosigar.FileSystem
	// We're reading the list of mounts in reverse order: we're assuming
	// that mounts are LIFO and can only be stacked, so the best match
	// will necessarily be the first filesystem that's a prefix of the
	// target directory, when looking from the end of the file.
	//
	// TODO(ssd): Steven points out that gosigar reads from /etc/mtab on
	// linux, which is sometimes managed by the user command 'mount' and
	// can sometimes miss entries when `mount -n` is used. It might be
	// better to change gosigar to use /proc/mounts instead.
	//
	// FWIW, we are OK with this for now, since the systems where crdb
	// is typically being deployed are well-behaved in that regard:
	// Kubernetes mirrors /proc/mount in /etc/mtab.
	for i := len(fslist.List) - 1; i >= 0; i-- {
		if pathIsInside(fslist.List[i].DirName, absPath) {
			fsInfo = &fslist.List[i]
			break
		}
	}
	if fsInfo == nil {
		// This is surprising!? We're expecting at least a match on the
		// root filesystem. Oh well.
		return fsprops
	}

	fsprops.FsType = fsInfo.SysTypeName
	fsprops.BlockDevice = fsInfo.DevName
	fsprops.MountPoint = fsInfo.DirName
	fsprops.MountOptions = fsInfo.Options
	return fsprops
}

// pathIsInside returns true if the absolute target path is inside a base path.
func pathIsInside(basePath string, absTargetPath string) bool {
	// filepath.Rel can reliably tell us if a path is relative to
	// another: if it is not, an error is returned.
	relPath, err := filepath.Rel(basePath, absTargetPath)
	if err != nil {
		return false
	}
	if strings.HasPrefix(relPath, "..") {
		// This check is consistent with internal filepath code (like isLocal).
		if len(relPath) == 2 || relPath[2] == filepath.Separator {
			return false
		}
	}
	return true
}
