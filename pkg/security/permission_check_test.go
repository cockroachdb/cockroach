// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows && !plan9

package security

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type keyFile struct {
	uid   uint32
	gid   uint32
	mode  os.FileMode
	valid bool
}

// we're not testing any code that interacts with this so let's lie
func (keyFile keyFile) Mode() os.FileMode            { return keyFile.mode }
func (keyFile keyFile) UID() uint64                  { return uint64(keyFile.uid) }
func (keyFile keyFile) GID() uint64                  { return uint64(keyFile.gid) }
func (keyFile keyFile) IsOwnedByUID(uid uint64) bool { return keyFile.UID() == uid }
func (keyFile keyFile) IsOwnedByGID(gid uint64) bool { return keyFile.GID() == gid }

func TestCheckFilePermission(t *testing.T) {
	defer leaktest.AfterTest(t)()

	processGID := uint32(1000)

	tests := []keyFile{
		{
			uid:   0,
			gid:   0,
			mode:  0700,
			valid: true,
		},
		{
			uid:   0,
			gid:   processGID,
			mode:  0740,
			valid: true,
		},
		{
			uid:   0,
			gid:   0,
			mode:  0700,
			valid: true,
		},
		{
			uid:   0,
			gid:   0,
			mode:  0770,
			valid: false,
		},
		{
			uid:   0,
			gid:   processGID,
			mode:  0640,
			valid: true,
		},
	}

	for i, testKeyFile := range tests {
		err := checkFilePermissions(int(processGID), "example file", testKeyFile)
		if testKeyFile.valid && err != nil {
			t.Errorf("key permission check %d failed when it should have passed with error: %s", i, err)
		} else if !testKeyFile.valid && err == nil {
			t.Errorf("key permission check %d should have failed but did not (permissions %s, uid %d, gid %d)", i, testKeyFile.Mode(), testKeyFile.uid, testKeyFile.gid)
		}
	}
}
