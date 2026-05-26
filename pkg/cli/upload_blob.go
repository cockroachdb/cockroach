// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// blobUploader uploads a single file to a blob storage backend.
// bytesTransferred is the count of bytes actually moved over the wire
// in this call (zero on the resume-already-complete path), distinct
// from the file's total size.
type blobUploader interface {
	Upload(ctx context.Context, srcPath string) (destPath string, bytesTransferred int64, err error)
	Close() error
}

var errSessionURIDead = errors.New("upload session has expired")

// gcsURIScheme is the URI prefix for Google Cloud Storage paths.
const gcsURIScheme = "gs://"

// maxErrorBody bounds how much of an HTTP error response we read into
// memory before giving up — caps memory exposure to a misbehaving
// server or proxy that returns multi-MB error bodies.
const maxErrorBody = 8 << 10 // 8 KiB

// readBoundedBody drains up to maxErrorBody bytes for inclusion in an
// error message. If the read itself fails, the failure is appended so
// "empty body" and "could not read body" aren't conflated.
func readBoundedBody(r io.Reader) string {
	body, err := io.ReadAll(io.LimitReader(r, maxErrorBody))
	msg := string(body)
	if err != nil {
		msg += fmt.Sprintf(" <body read error: %v>", err)
	}
	return msg
}

type gcsSessionURIUploader struct {
	sessionURI  string
	bucket      string
	objectPath  string
	contentType string
	httpClient  *http.Client
	resuming    bool
}

// newBlobUploader is a var so tests can substitute a mock.
var newBlobUploader = func(
	_ context.Context,
	sessionURI, bucket, objectPath, contentType string,
	httpClient *http.Client,
	resuming bool,
) (blobUploader, error) {
	if sessionURI == "" {
		return nil, errors.New("session URI is empty")
	}
	if httpClient == nil {
		return nil, errors.New("http client is nil")
	}
	return &gcsSessionURIUploader{
		sessionURI:  sessionURI,
		bucket:      bucket,
		objectPath:  objectPath,
		contentType: contentType,
		httpClient:  httpClient,
		resuming:    resuming,
	}, nil
}

func (u *gcsSessionURIUploader) Upload(ctx context.Context, srcPath string) (string, int64, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return "", 0, errors.Wrap(err, "opening zip file")
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return "", 0, errors.Wrap(err, "stat zip file")
	}
	totalSize := fi.Size()

	var offset int64
	if u.resuming {
		probe, err := u.queryUploadOffset(ctx, totalSize)
		if err != nil {
			return "", 0, err
		}
		if probe.complete {
			fmt.Fprintf(os.Stderr, "already fully uploaded; nothing to send.\n")
			return u.destURI(), 0, nil
		}
		offset = probe.offset
		if offset > 0 {
			fmt.Fprintf(os.Stderr,
				"resuming upload at byte %d / %d (%.1f%% already on CRL support)\n",
				offset, totalSize, float64(offset)/float64(totalSize)*100,
			)
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				return "", 0, errors.Wrapf(err, "seeking to resume offset %d", offset)
			}
		}
	}

	body := &progressReader{r: f, total: totalSize, written: offset}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.sessionURI, body)
	if err != nil {
		return "", 0, errors.Wrap(err, "building PUT request")
	}
	// Without an explicit ContentLength the http client uses chunked
	// transfer encoding, which GCS rejects on resumable session URIs.
	req.ContentLength = totalSize - offset
	if u.contentType != "" {
		req.Header.Set("Content-Type", u.contentType)
	}
	if offset > 0 {
		// "bytes <first>-<last>/<total>"; last is inclusive.
		req.Header.Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", offset, totalSize-1, totalSize))
	}

	resp, err := doCRLHTTPRequest(u.httpClient, req)
	if err != nil {
		return "", 0, errors.Wrap(err, "uploading file")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", 0, errors.Newf(
			"blob upload failed (%d): %s", resp.StatusCode, readBoundedBody(resp.Body),
		)
	}
	return u.destURI(), totalSize - offset, nil
}

// uploadOffsetProbe is the result of probing GCS for how many bytes
// it already has of an in-progress resumable upload. complete is set
// when the object is fully uploaded; otherwise offset is the byte
// from which to resume.
type uploadOffsetProbe struct {
	offset   int64
	complete bool
}

// queryUploadOffset asks GCS how many bytes it has of the session URI's
// object. Returns {totalSize, true} if the object is already complete,
// {offset, false} to resume at byte `offset`, or errSessionURIDead if
// the URI is gone (404/410).
//
// https://cloud.google.com/storage/docs/performing-resumable-uploads#status-check
func (u *gcsSessionURIUploader) queryUploadOffset(
	ctx context.Context, totalSize int64,
) (uploadOffsetProbe, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.sessionURI, nil)
	if err != nil {
		return uploadOffsetProbe{}, errors.Wrap(err, "building probe request")
	}
	req.ContentLength = 0
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))

	resp, err := doCRLHTTPRequest(u.httpClient, req)
	if err != nil {
		return uploadOffsetProbe{}, errors.Wrap(err, "probing upload offset")
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused for the subsequent PUT.
	defer func() { _, _ = io.Copy(io.Discard, resp.Body) }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return uploadOffsetProbe{offset: totalSize, complete: true}, nil

	case http.StatusPermanentRedirect: // 308 Resume Incomplete
		rangeHdr := resp.Header.Get("Range")
		if rangeHdr == "" {
			return uploadOffsetProbe{}, nil
		}
		offset, err := parseResumeRangeHeader(rangeHdr)
		if err != nil {
			return uploadOffsetProbe{}, errors.Wrapf(err,
				"parsing GCS Range header %q", rangeHdr)
		}
		return uploadOffsetProbe{offset: offset}, nil

	case http.StatusNotFound, http.StatusGone:
		return uploadOffsetProbe{}, errSessionURIDead

	default:
		return uploadOffsetProbe{}, errors.Newf(
			"probe failed (%d): %s", resp.StatusCode, readBoundedBody(resp.Body),
		)
	}
}

// parseResumeRangeHeader extracts N+1 from a GCS Range header of the
// form "bytes=0-N". Other forms are rejected — GCS isn't documented to
// send them and we don't want to guess.
func parseResumeRangeHeader(v string) (int64, error) {
	v = strings.TrimPrefix(v, "bytes=")
	dash := strings.IndexByte(v, '-')
	if dash < 0 {
		return 0, errors.Newf("missing '-' in range %q", v)
	}
	if v[:dash] != "0" {
		return 0, errors.Newf("unexpected range start %q in %q", v[:dash], v)
	}
	last, err := strconv.ParseInt(v[dash+1:], 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing range end")
	}
	if last < 0 {
		return 0, errors.Newf("negative range end %d in %q", last, v)
	}
	return last + 1, nil
}

func (u *gcsSessionURIUploader) destURI() string {
	return fmt.Sprintf("%s%s/%s", gcsURIScheme, u.bucket, u.objectPath)
}

func (u *gcsSessionURIUploader) Close() error { return nil }

// progressReader prints upload progress to stderr. `written` starts at
// the resume offset so the percentage covers the whole file, not just
// this attempt.
type progressReader struct {
	r       io.Reader
	total   int64
	written int64
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 {
		pr.written += int64(n)
		if pr.total > 0 {
			pct := float64(pr.written) / float64(pr.total) * 100
			fmt.Fprintf(os.Stderr, "\rUploading... %s / %s (%.1f%%)",
				humanReadableSize(int(pr.written)),
				humanReadableSize(int(pr.total)),
				pct,
			)
			if pr.written >= pr.total {
				fmt.Fprintln(os.Stderr)
			}
		}
	}
	return n, err
}
