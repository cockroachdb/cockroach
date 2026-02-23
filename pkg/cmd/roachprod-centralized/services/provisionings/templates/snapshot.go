// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	// maxSnapshotSize is the maximum allowed size for a template snapshot
	// archive (10MB). Templates are typically <100KB compressed.
	maxSnapshotSize = 10 * 1024 * 1024

	// extractCompleteMarker is written after a successful extraction. If the
	// marker is absent when ExtractSnapshot is called, any partial contents
	// are removed and the archive is re-extracted from scratch.
	extractCompleteMarker = ".extract_complete"
)

// SnapshotTemplate creates a tar.gz archive of the template identified by
// directory name or metadata name. Symlinks are resolved and the actual files
// are included. Returns the archive bytes and the SHA256 checksum.
func (m *Manager) SnapshotTemplate(name string) ([]byte, string, error) {
	// Resolve to the actual template directory (supports both dir name and
	// metadata name).
	tmpl, err := m.GetTemplate(name)
	if err != nil {
		return nil, "", err
	}
	templateDir := tmpl.Path

	templatesRoot, err := filepath.EvalSymlinks(m.templatesDir)
	if err != nil {
		return nil, "", errors.Wrap(err, "resolve templates root")
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	walkErr := filepath.Walk(templateDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		relPath, err := filepath.Rel(templateDir, path)
		if err != nil {
			return errors.Wrapf(err, "rel path for %s", path)
		}

		// Include template.yaml/template.yml in the archive — hooks and SSH
		// config declared in the marker are needed for post-provisioning
		// actions and manual trigger replay.

		// Resolve symlinks: follow them and include the actual files.
		realPath := path
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, resolveErr := resolveSymlink(path, templatesRoot)
			if resolveErr != nil {
				return errors.Wrapf(resolveErr, "resolve symlink %s", relPath)
			}
			realPath = resolved
			realInfo, statErr := os.Stat(realPath)
			if statErr != nil {
				return errors.Wrapf(statErr, "stat resolved symlink %s", relPath)
			}
			info = realInfo

			// If the symlink points to a directory, walk it recursively.
			if info.IsDir() {
				return addDirToTar(tw, realPath, relPath, templatesRoot, make(map[string]bool))
			}
		}

		if info.IsDir() {
			return nil
		}

		return addFileToTar(tw, realPath, relPath, info)
	})
	if walkErr != nil {
		return nil, "", errors.Wrap(walkErr, "walk template directory")
	}

	if err := tw.Close(); err != nil {
		return nil, "", errors.Wrap(err, "close tar writer")
	}
	if err := gz.Close(); err != nil {
		return nil, "", errors.Wrap(err, "close gzip writer")
	}

	archive := buf.Bytes()
	if len(archive) > maxSnapshotSize {
		return nil, "", errors.Newf(
			"template snapshot too large: %d bytes (max %d)", len(archive), maxSnapshotSize,
		)
	}

	checksum := fmt.Sprintf("%x", sha256.Sum256(archive))
	return archive, checksum, nil
}

// addDirToTar recursively adds all files from a directory into the tar at the
// given prefix path. visited tracks resolved absolute paths of directories
// already added to break cycles caused by directory symlinks.
func addDirToTar(
	tw *tar.Writer, srcDir, prefix, templatesRoot string, visited map[string]bool,
) error {
	absDir, err := filepath.Abs(srcDir)
	if err != nil {
		return errors.Wrapf(err, "abs path for %s", srcDir)
	}
	if visited[absDir] {
		return nil
	}
	visited[absDir] = true

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relToSrc, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		archivePath := filepath.Join(prefix, relToSrc)

		// Resolve any nested symlinks.
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, resolveErr := resolveSymlink(path, templatesRoot)
			if resolveErr != nil {
				return resolveErr
			}
			realInfo, statErr := os.Stat(resolved)
			if statErr != nil {
				return statErr
			}
			if realInfo.IsDir() {
				return addDirToTar(tw, resolved, archivePath, templatesRoot, visited)
			}
			return addFileToTar(tw, resolved, archivePath, realInfo)
		}

		if info.IsDir() {
			return nil
		}

		return addFileToTar(tw, path, archivePath, info)
	})
}

// addFileToTar adds a single file to the tar archive.
func addFileToTar(tw *tar.Writer, srcPath, archivePath string, info os.FileInfo) error {
	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return errors.Wrapf(err, "create tar header for %s", archivePath)
	}
	header.Name = archivePath

	// Normalize metadata so that identical file content produces identical
	// archive bytes regardless of when or where the snapshot is built.
	header.ModTime = time.Time{}
	header.AccessTime = time.Time{}
	header.ChangeTime = time.Time{}
	header.Uid = 0
	header.Gid = 0
	header.Uname = ""
	header.Gname = ""

	if err := tw.WriteHeader(header); err != nil {
		return errors.Wrapf(err, "write tar header for %s", archivePath)
	}

	f, err := os.Open(srcPath)
	if err != nil {
		return errors.Wrapf(err, "open %s", srcPath)
	}
	defer f.Close()

	if _, err := io.Copy(tw, f); err != nil {
		return errors.Wrapf(err, "copy %s to tar", archivePath)
	}

	return nil
}

// resolveSymlink resolves a symlink target and validates that it stays within
// templatesRoot. Circular symlink loops are detected by filepath.EvalSymlinks,
// which returns an error if resolution does not converge.
func resolveSymlink(link, templatesRoot string) (string, error) {
	target, err := os.Readlink(link)
	if err != nil {
		return "", errors.Wrapf(err, "readlink %s", link)
	}

	// Resolve relative symlinks against the link's parent directory.
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(link), target)
	}

	resolved, err := filepath.EvalSymlinks(target)
	if err != nil {
		return "", errors.Wrapf(err, "eval symlinks for %s", target)
	}

	// Security: ensure the resolved target is within the templates root.
	absRoot, err := filepath.Abs(templatesRoot)
	if err != nil {
		return "", errors.Wrap(err, "abs templates root")
	}
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return "", errors.Wrap(err, "abs resolved path")
	}

	if !strings.HasPrefix(absResolved, absRoot+string(filepath.Separator)) && absResolved != absRoot {
		return "", errors.Newf(
			"symlink %s resolves outside templates directory: %s", link, absResolved,
		)
	}

	return resolved, nil
}

// ExtractSnapshot extracts a tar.gz archive into workingDir. The extraction is
// idempotent: a completion marker is written after successful extraction. If
// the marker is present, extraction is skipped. If the directory exists without
// the marker (e.g., a previous extraction was interrupted), the directory is
// removed and extraction starts fresh.
func ExtractSnapshot(archive []byte, workingDir string) error {
	marker := filepath.Join(workingDir, extractCompleteMarker)

	// If the marker exists, a previous extraction completed successfully.
	if _, err := os.Stat(marker); err == nil {
		return nil
	}

	// No marker — remove any partial contents and start fresh.
	if err := os.RemoveAll(workingDir); err != nil {
		return errors.Wrapf(err, "remove partial working directory %s", workingDir)
	}

	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		return errors.Wrapf(err, "create working directory %s", workingDir)
	}

	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return errors.Wrap(err, "open gzip reader")
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read tar entry")
		}

		target := filepath.Join(workingDir, header.Name)

		// Security: prevent path traversal via crafted tar entries.
		absTarget, err := filepath.Abs(target)
		if err != nil {
			return errors.Wrapf(err, "abs target path for %s", header.Name)
		}
		absWorkingDir, err := filepath.Abs(workingDir)
		if err != nil {
			return errors.Wrap(err, "abs working directory")
		}
		if !strings.HasPrefix(absTarget, absWorkingDir+string(filepath.Separator)) {
			return errors.Newf("path traversal in tar: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return errors.Wrapf(err, "create dir %s", target)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return errors.Wrapf(err, "create parent dir for %s", target)
			}
			f, err := os.Create(target)
			if err != nil {
				return errors.Wrapf(err, "create file %s", target)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return errors.Wrapf(err, "extract file %s", target)
			}
			f.Close()
		}
	}

	// Write the completion marker only after all files are extracted.
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		return errors.Wrap(err, "write extraction completion marker")
	}

	return nil
}
