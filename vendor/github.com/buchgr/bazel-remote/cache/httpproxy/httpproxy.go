// Package httpproxy is a cache implementation that can proxy artifacts
// from/to another HTTP-based remote cache.
package httpproxy

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/buchgr/bazel-remote/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type uploadReq struct {
	hash string
	size int64
	kind cache.EntryKind
	rc   io.ReadCloser
}

type remoteHTTPProxyCache struct {
	remote       *http.Client
	baseURL      *url.URL
	uploadQueue  chan<- uploadReq
	accessLogger cache.Logger
	errorLogger  cache.Logger
}

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_http_cache_hits",
		Help: "The total number of HTTP backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_http_cache_misses",
		Help: "The total number of HTTP backend cache misses",
	})
)

func uploadFile(remote *http.Client, baseURL *url.URL, accessLogger cache.Logger,
	errorLogger cache.Logger, item uploadReq) {

	if item.size == 0 {
		item.rc.Close()
		// See https://github.com/golang/go/issues/20257#issuecomment-299509391
		item.rc = http.NoBody
	}

	url := requestURL(baseURL, item.hash, item.kind)

	rsp, err := remote.Head(url)
	if err == nil && rsp.StatusCode == http.StatusOK {
		accessLogger.Printf("SKIP UPLOAD %s", item.hash)
		return
	}

	req, err := http.NewRequest(http.MethodPut, url, item.rc)
	if err != nil {
		// item.rc will be closed if we call req.Do(), but not if we
		// return earlier.
		item.rc.Close()

		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = item.size

	rsp, err = remote.Do(req)
	if err != nil {
		return
	}
	io.Copy(ioutil.Discard, rsp.Body)
	rsp.Body.Close()

	logResponse(accessLogger, "UPLOAD", rsp.StatusCode, url)
	return
}

// New creates a cache that proxies requests to a HTTP remote cache.
func New(baseURL *url.URL, remote *http.Client, accessLogger cache.Logger,
	errorLogger cache.Logger, numUploaders, maxQueuedUploads int) cache.Proxy {

	proxy := &remoteHTTPProxyCache{
		remote:       remote,
		baseURL:      baseURL,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}

	if maxQueuedUploads > 0 && numUploaders > 0 {
		uploadQueue := make(chan uploadReq, maxQueuedUploads)

		for i := 0; i < numUploaders; i++ {
			go func(remote *http.Client, baseURL *url.URL, accessLogger cache.Logger,
				errorLogger cache.Logger) {
				for item := range uploadQueue {
					uploadFile(remote, baseURL, accessLogger, errorLogger, item)
				}
			}(remote, baseURL, accessLogger, errorLogger)
		}

		proxy.uploadQueue = uploadQueue
	}

	return proxy
}

// Helper function for logging responses
func logResponse(logger cache.Logger, method string, code int, url string) {
	logger.Printf("HTTP %s %d %s", method, code, url)
}

func (r *remoteHTTPProxyCache) Put(kind cache.EntryKind, hash string, size int64, rc io.ReadCloser) {
	if r.uploadQueue == nil {
		rc.Close()
		return
	}

	select {
	case r.uploadQueue <- uploadReq{
		hash: hash,
		size: size,
		kind: kind,
		rc:   rc,
	}:
	default:
		r.errorLogger.Printf("too many uploads queued")
		rc.Close()
	}
}

func (r *remoteHTTPProxyCache) Get(kind cache.EntryKind, hash string) (io.ReadCloser, int64, error) {
	url := requestURL(r.baseURL, hash, kind)
	rsp, err := r.remote.Get(url)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}

	logResponse(r.accessLogger, "DOWNLOAD", rsp.StatusCode, url)

	if rsp.StatusCode == http.StatusNotFound {
		cacheMisses.Inc()
		return nil, -1, nil
	}

	if rsp.StatusCode != http.StatusOK {
		// If the failed http response contains some data then
		// forward up to 1 KiB.
		var errorBytes []byte
		errorBytes, err = ioutil.ReadAll(io.LimitReader(rsp.Body, 1024))
		var errorText string
		if err == nil {
			errorText = string(errorBytes)
		}

		cacheMisses.Inc()
		return nil, -1, &cache.Error{
			Code: rsp.StatusCode,
			Text: errorText,
		}
	}

	sizeBytesStr := rsp.Header.Get("Content-Length")
	if sizeBytesStr == "" {
		err = errors.New("Missing Content-Length header")
		cacheMisses.Inc()
		return nil, -1, err
	}

	sizeBytesInt, err := strconv.Atoi(sizeBytesStr)
	if err != nil {
		cacheMisses.Inc()
		return nil, -1, err
	}
	sizeBytes := int64(sizeBytesInt)

	cacheHits.Inc()

	return rsp.Body, sizeBytes, err
}

func (r *remoteHTTPProxyCache) Contains(kind cache.EntryKind, hash string) (bool, int64) {

	url := requestURL(r.baseURL, hash, kind)

	rsp, err := r.remote.Head(url)
	if err == nil && rsp.StatusCode == http.StatusOK {
		return true, rsp.ContentLength
	}

	return false, int64(-1)
}

func requestURL(baseURL *url.URL, hash string, kind cache.EntryKind) string {
	return fmt.Sprintf("%s/%s/%s", baseURL, kind, hash)
}
