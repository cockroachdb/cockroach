package stream

import (
	"os"
	"path/filepath"
)

// FindFilter is a filter that produces matching nodes under a filesystem
// directory.
type FindFilter struct {
	dir       string
	ifmode    func(os.FileMode) bool
	skipdirif func(string) bool
}

// Find returns a filter that produces matching nodes under a
// filesystem directory. The items yielded by the filter will be
// prefixed by dir. E.g., if dir contains subdir/file, the filter
// will yield dir/subdir/file. By default, the filter matches all types
// of files (regular files, directories, symbolic links, etc.).
// This behavior can be adjusted by calling FindFilter methods
// before executing the filter.
func Find(dir string) *FindFilter {
	return &FindFilter{
		dir:       dir,
		ifmode:    func(os.FileMode) bool { return true },
		skipdirif: func(d string) bool { return false },
	}
}

// IfMode adjusts f so it only matches nodes for which fn(mode) returns true.
func (f *FindFilter) IfMode(fn func(os.FileMode) bool) *FindFilter {
	f.ifmode = fn
	return f
}

// SkipDirIf adjusts f so that if fn(d) returns true for a directory d,
// d and all of d's descendents are skipped.
func (f *FindFilter) SkipDirIf(fn func(d string) bool) *FindFilter {
	f.skipdirif = fn
	return f
}

// RunFilter yields contents of a filesystem tree. It implements the
// Filter interface.
func (f *FindFilter) RunFilter(arg Arg) error {
	return filepath.Walk(f.dir, func(n string, s os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if s.Mode().IsDir() && f.skipdirif(n) {
			return filepath.SkipDir
		}
		if f.ifmode(s.Mode()) {
			arg.Out <- n
		}
		return nil
	})
}
