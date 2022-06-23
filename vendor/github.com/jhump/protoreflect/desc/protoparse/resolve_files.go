package protoparse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errNoImportPathsForAbsoluteFilePath = errors.New("must specify at least one import path if any absolute file paths are given")

// ResolveFilenames tries to resolve fileNames into paths that are relative to
// directories in the given importPaths. The returned slice has the results in
// the same order as they are supplied in fileNames.
//
// The resulting names should be suitable for passing to Parser.ParseFiles.
//
// If no import paths are given and any file name is absolute, this returns an
// error.  If no import paths are given and all file names are relative, this
// returns the original file names. If a file name is already relative to one
// of the given import paths, it will be unchanged in the returned slice. If a
// file name given is relative to the current working directory, it will be made
// relative to one of the given import paths; but if it cannot be made relative
// (due to no matching import path), an error will be returned.
func ResolveFilenames(importPaths []string, fileNames ...string) ([]string, error) {
	if len(importPaths) == 0 {
		if containsAbsFilePath(fileNames) {
			// We have to do this as otherwise parseProtoFiles can result in duplicate symbols.
			// For example, assume we import "foo/bar/bar.proto" in a file "/home/alice/dev/foo/bar/baz.proto"
			// as we call ParseFiles("/home/alice/dev/foo/bar/bar.proto","/home/alice/dev/foo/bar/baz.proto")
			// with "/home/alice/dev" as our current directory. Due to the recursive nature of parseProtoFiles,
			// it will discover the import "foo/bar/bar.proto" in the input file, and call parse on this,
			// adding "foo/bar/bar.proto" to the parsed results, as well as "/home/alice/dev/foo/bar/bar.proto"
			// from the input file list. This will result in a
			// 'duplicate symbol SYMBOL: already defined as field in "/home/alice/dev/foo/bar/bar.proto'
			// error being returned from ParseFiles.
			return nil, errNoImportPathsForAbsoluteFilePath
		}
		return fileNames, nil
	}
	absImportPaths, err := absoluteFilePaths(importPaths)
	if err != nil {
		return nil, err
	}
	resolvedFileNames := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		resolvedFileName, err := resolveFilename(absImportPaths, fileName)
		if err != nil {
			return nil, err
		}
		// On Windows, the resolved paths will use "\", but proto imports
		// require the use of "/". So fix up here.
		if filepath.Separator != '/' {
			resolvedFileName = strings.Replace(resolvedFileName, string(filepath.Separator), "/", -1)
		}
		resolvedFileNames = append(resolvedFileNames, resolvedFileName)
	}
	return resolvedFileNames, nil
}

func containsAbsFilePath(filePaths []string) bool {
	for _, filePath := range filePaths {
		if filepath.IsAbs(filePath) {
			return true
		}
	}
	return false
}

func absoluteFilePaths(filePaths []string) ([]string, error) {
	absFilePaths := make([]string, 0, len(filePaths))
	for _, filePath := range filePaths {
		absFilePath, err := canonicalize(filePath)
		if err != nil {
			return nil, err
		}
		absFilePaths = append(absFilePaths, absFilePath)
	}
	return absFilePaths, nil
}

func canonicalize(filePath string) (string, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return "", err
	}
	// this is kind of gross, but it lets us construct a resolved path even if some
	// path elements do not exist (a single call to filepath.EvalSymlinks would just
	// return an error, ENOENT, in that case).
	head := absPath
	tail := ""
	for {
		noLinks, err := filepath.EvalSymlinks(head)
		if err == nil {
			if tail != "" {
				return filepath.Join(noLinks, tail), nil
			}
			return noLinks, nil
		}

		if tail == "" {
			tail = filepath.Base(head)
		} else {
			tail = filepath.Join(filepath.Base(head), tail)
		}
		head = filepath.Dir(head)
		if head == "." {
			// ran out of path elements to try to resolve
			return absPath, nil
		}
	}
}

const dotPrefix = "." + string(filepath.Separator)
const dotDotPrefix = ".." + string(filepath.Separator)

func resolveFilename(absImportPaths []string, fileName string) (string, error) {
	if filepath.IsAbs(fileName) {
		return resolveAbsFilename(absImportPaths, fileName)
	}

	if !strings.HasPrefix(fileName, dotPrefix) && !strings.HasPrefix(fileName, dotDotPrefix) {
		// Use of . and .. are assumed to be relative to current working
		// directory. So if those aren't present, check to see if the file is
		// relative to an import path.
		for _, absImportPath := range absImportPaths {
			absFileName := filepath.Join(absImportPath, fileName)
			_, err := os.Stat(absFileName)
			if err != nil {
				continue
			}
			// found it! it was relative to this import path
			return fileName, nil
		}
	}

	// must be relative to current working dir
	return resolveAbsFilename(absImportPaths, fileName)
}

func resolveAbsFilename(absImportPaths []string, fileName string) (string, error) {
	absFileName, err := canonicalize(fileName)
	if err != nil {
		return "", err
	}
	for _, absImportPath := range absImportPaths {
		if isDescendant(absImportPath, absFileName) {
			resolvedPath, err := filepath.Rel(absImportPath, absFileName)
			if err != nil {
				return "", err
			}
			return resolvedPath, nil
		}
	}
	return "", fmt.Errorf("%s does not reside in any import path", fileName)
}

// isDescendant returns true if file is a descendant of dir. Both dir and file must
// be cleaned, absolute paths.
func isDescendant(dir, file string) bool {
	dir = filepath.Clean(dir)
	cur := file
	for {
		d := filepath.Dir(cur)
		if d == dir {
			return true
		}
		if d == "." || d == cur {
			// we've run out of path elements
			return false
		}
		cur = d
	}
}
