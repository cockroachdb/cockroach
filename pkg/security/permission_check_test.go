// +build !windows,!plan9

package security

import (
	"os"
	"syscall"
	"testing"
	"time"
)

type keyFile struct {
	name        string
	uid         uint32
	gid         uint32
	permissions os.FileMode
	valid       bool
}

// we're not testing any code that interacts with this so let's lie
func (keyFile keyFile) Size() int64        { return int64(10) }
func (keyFile keyFile) ModTime() time.Time { return time.Now() }
func (keyFile keyFile) IsDir() bool        { return false }

// the actual functionality we need to test
func (keyFile keyFile) Mode() os.FileMode { return keyFile.permissions }

func (keyFile keyFile) Name() string { return keyFile.name }

func (keyFile keyFile) Sys() interface{} {
	stat := &syscall.Stat_t{
		Uid:  keyFile.uid,
		Gid:  keyFile.gid,
		Mode: uint16(keyFile.permissions),
	}
	return stat
}

func TestCheckFilePermission(t *testing.T) {
	processGID := uint32(1000)

	tests := []keyFile{
		{
			uid:         0,
			gid:         0,
			permissions: 0700,
			valid:       true,
		},
		{
			uid:         0,
			gid:         processGID,
			permissions: 0740,
			valid:       true,
		},
		{
			uid:         0,
			gid:         0,
			permissions: 0700,
			valid:       true,
		},
		{
			uid:         0,
			gid:         0,
			permissions: 0770,
			valid:       false,
		},
		{
			uid:         0,
			gid:         processGID,
			permissions: 0640,
			valid:       true,
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
