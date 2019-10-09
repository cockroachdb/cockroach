package engine

import (
	"bytes"
	"github.com/cockroachdb/pebble/vfs"
	"io"
)

func SafeWriteToFile(fs vfs.FS, filename string, b []byte) error {
	tempName := filename + ".crdbtmp"
	f, err := fs.Create(tempName)
	if err != nil {
		return err
	}
	bReader := bytes.NewReader(b)
	if _, err = io.Copy(f, bReader); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = fs.Rename(tempName, filename); err != nil {
		return err
	}
	fdir, err := fs.OpenDir(fs.PathDir(filename))
	if err != nil {
		return err
	}
	defer fdir.Close()
	if err := fdir.Sync(); err != nil {
		return err
	}
	return nil
}
