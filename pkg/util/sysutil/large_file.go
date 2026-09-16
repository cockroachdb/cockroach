// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sysutil

import (
	"math/rand"
	"os"
	"time"

	"github.com/cockroachdb/errors"
)

func resizeLargeFileNaive(path string, bytes int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	// Fill the file with pseudo-random data rather than zeroes. Some
	// filesystems (eg, ZFS with compression enabled) compress long runs of
	// zeroes down to almost nothing, defeating the purpose of a ballast file:
	// reserving real, reclaimable disk space (see #78606). Random data is
	// effectively incompressible, forcing the filesystem to actually
	// allocate the requested space. The data need not be cryptographically
	// random, only resistant to compression, so a single buffer generated
	// once and reused for the whole file keeps large ballasts fast to
	// create.
	sixtyFourMB := make([]byte, 64<<20)
	_, _ = rand.New(rand.NewSource(time.Now().UnixNano())).Read(sixtyFourMB) // (*rand.Rand).Read never errors
	for bytes > 0 {
		z := sixtyFourMB
		if bytes < int64(len(z)) {
			z = sixtyFourMB[:bytes]
		}
		if _, err := f.Write(z); err != nil {
			return errors.Wrap(err, "write")
		}
		bytes -= int64(len(z))
	}
	return errors.Wrap(f.Sync(), "fsync")
}
