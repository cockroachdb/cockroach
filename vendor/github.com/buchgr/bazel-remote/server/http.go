package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/buchgr/bazel-remote/cache"
	"github.com/buchgr/bazel-remote/cache/disk"
	pb "github.com/buchgr/bazel-remote/genproto/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

var blobNameSHA256 = regexp.MustCompile("^/?(.*/)?(ac/|cas/)([a-f0-9]{64})$")

// HTTPCache ...
type HTTPCache interface {
	CacheHandler(w http.ResponseWriter, r *http.Request)
	StatusPageHandler(w http.ResponseWriter, r *http.Request)
}

type httpCache struct {
	cache        *disk.Cache
	accessLogger cache.Logger
	errorLogger  cache.Logger
	validateAC   bool
	mangleACKeys bool
	gitCommit    string
}

type statusPageData struct {
	CurrSize      int64
	ReservedSize  int64
	MaxSize       int64
	NumFiles      int
	ServerTime    int64
	GitCommit     string
	NumGoroutines int
}

// NewHTTPCache returns a new instance of the cache.
// accessLogger will print one line for each HTTP request to stdout.
// errorLogger will print unexpected server errors. Inexistent files and malformed URLs will not
// be reported.
func NewHTTPCache(cache *disk.Cache, accessLogger cache.Logger, errorLogger cache.Logger, validateAC bool, mangleACKeys bool, commit string) HTTPCache {

	_, _, numItems := cache.Stats()

	errorLogger.Printf("Loaded %d existing disk cache items.", numItems)

	hc := &httpCache{
		cache:        cache,
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
		validateAC:   validateAC,
		mangleACKeys: mangleACKeys,
	}

	if commit != "{STABLE_GIT_COMMIT}" {
		hc.gitCommit = commit
	}

	return hc
}

// Parse cache artifact information from the request URL
func parseRequestURL(url string, validateAC bool) (kind cache.EntryKind, hash string, instance string, err error) {
	m := blobNameSHA256.FindStringSubmatch(url)
	if m == nil {
		err := fmt.Errorf("resource name must be a SHA256 hash in hex. "+
			"got '%s'", html.EscapeString(url))
		return 0, "", "", err
	}

	instance = strings.TrimSuffix(m[1], "/")

	parts := m[2:]
	if len(parts) != 2 {
		err := fmt.Errorf("the path '%s' is invalid. expected (ac/|cas/)sha256",
			html.EscapeString(url))
		return 0, "", "", err
	}

	// The regex ensures that parts[0] can only be "ac/" or "cas/"
	hash = parts[1]
	if parts[0] == "cas/" {
		return cache.CAS, hash, instance, nil
	}

	if validateAC {
		return cache.AC, hash, instance, nil
	}

	return cache.RAW, hash, instance, nil
}
func (h *httpCache) handleContainsValidAC(w http.ResponseWriter, r *http.Request, hash string) {
	_, data, err := h.cache.GetValidatedActionResult(hash)
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		h.logResponse(http.StatusNotFound, r)
		return
	}

	if data == nil {
		http.Error(w, "Not found", http.StatusNotFound)
		h.logResponse(http.StatusNotFound, r)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	w.WriteHeader(http.StatusOK)
	h.logResponse(http.StatusOK, r)
}

func (h *httpCache) handleGetValidAC(w http.ResponseWriter, r *http.Request, hash string) {
	_, data, err := h.cache.GetValidatedActionResult(hash)
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		h.logResponse(http.StatusNotFound, r)
		return
	}

	if data == nil {
		http.Error(w, "Not found", http.StatusNotFound)
		h.logResponse(http.StatusNotFound, r)
		return
	}

	if r.Header.Get("Accept") == "application/json" {
		ar := &pb.ActionResult{}
		err = proto.Unmarshal(data, ar)
		if err != nil {
			h.logResponse(http.StatusInternalServerError, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		marshaler := jsonpb.Marshaler{}
		err = marshaler.Marshal(w, ar)
		if err != nil {
			h.logResponse(http.StatusInternalServerError, r)
			return
		}

		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	bytesWritten, err := w.Write(data)

	if err != nil {
		h.logResponse(http.StatusInternalServerError, r)
		return
	}

	if bytesWritten != len(data) {
		h.logResponse(http.StatusInternalServerError, r)
		return
	}
}

// Helper function for logging responses
func (h *httpCache) logResponse(code int, r *http.Request) {
	// Parse the client ip:port
	var clientAddress string
	var err error
	clientAddress, _, err = net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientAddress = r.RemoteAddr
	}
	h.accessLogger.Printf("%4s %d %15s %s", r.Method, code, clientAddress, r.URL.Path)
}

func (h *httpCache) CacheHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	kind, hash, instance, err := parseRequestURL(r.URL.Path, h.validateAC)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		h.logResponse(http.StatusBadRequest, r)
		return
	}

	if h.mangleACKeys && kind == cache.AC {
		hash = cache.TransformActionCacheKey(hash, instance, h.accessLogger)
	}

	switch m := r.Method; m {
	case http.MethodGet:

		if h.validateAC && kind == cache.AC {
			h.handleGetValidAC(w, r, hash)
			return
		}

		rdr, sizeBytes, err := h.cache.Get(kind, hash, -1)
		if err != nil {
			if e, ok := err.(*cache.Error); ok {
				http.Error(w, e.Error(), e.Code)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			h.errorLogger.Printf("GET %s: %s", path(kind, hash), err)
			return
		}

		if rdr == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			h.logResponse(http.StatusNotFound, r)
			return
		}
		defer rdr.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(sizeBytes, 10))
		io.Copy(w, rdr)

		h.logResponse(http.StatusOK, r)

	case http.MethodPut:
		contentLength := r.ContentLength

		if contentLength == -1 {
			// We need the content-length header to make sure we have enough disk space.
			msg := fmt.Sprintf("PUT without Content-Length (key = %s)", path(kind, hash))
			http.Error(w, msg, http.StatusBadRequest)
			h.errorLogger.Printf("PUT %s: %s", path(kind, hash), msg)
			return
		}

		if contentLength == 0 && kind == cache.CAS && hash != emptySha256 {
			msg := fmt.Sprintf("Invalid empty blob hash: \"%s\"", hash)
			http.Error(w, msg, http.StatusBadRequest)
			h.errorLogger.Printf("PUT %s: %s", path(kind, hash), msg)
			return
		}

		rc := r.Body
		if h.validateAC && kind == cache.AC {
			// verify that this is a valid ActionResult

			data, err := ioutil.ReadAll(rc)
			if err != nil {
				msg := "failed to read request body"
				http.Error(w, msg, http.StatusInternalServerError)
				h.errorLogger.Printf("PUT %s: %s", path(kind, hash), msg)
				return
			}

			if int64(len(data)) != contentLength {
				msg := fmt.Sprintf("sizes don't match. Expected %d, found %d",
					contentLength, len(data))
				http.Error(w, msg, http.StatusBadRequest)
				h.errorLogger.Printf("PUT %s: %s", path(kind, hash), msg)
				return
			}

			// Ensure that the serialized ActionResult has non-zero length.
			data, code, err := addWorkerMetadataHTTP(r.RemoteAddr, r.Header.Get("Content-Type"), data)
			if err != nil {
				http.Error(w, err.Error(), code)
				h.errorLogger.Printf("PUT %s: %s", path(kind, hash), err.Error())
				return
			}
			contentLength = int64(len(data))

			// Note: we do not currently verify that the blobs exist
			// in the CAS.

			rc = ioutil.NopCloser(bytes.NewReader(data))
		}

		err := h.cache.Put(kind, hash, contentLength, rc)
		if err != nil {
			if cerr, ok := err.(*cache.Error); ok {
				http.Error(w, err.Error(), cerr.Code)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			h.errorLogger.Printf("PUT %s: %s", path(kind, hash), err)
		} else {
			h.logResponse(http.StatusOK, r)
		}

	case http.MethodHead:

		if h.validateAC && kind == cache.AC {
			h.handleContainsValidAC(w, r, hash)
			return
		}

		// Unvalidated path:

		ok, size := h.cache.Contains(kind, hash, -1)
		if !ok {
			http.Error(w, "Not found", http.StatusNotFound)
			h.logResponse(http.StatusNotFound, r)
			return
		}

		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		h.logResponse(http.StatusOK, r)

	default:
		msg := fmt.Sprintf("Method '%s' not supported.", html.EscapeString(m))
		http.Error(w, msg, http.StatusMethodNotAllowed)
		h.logResponse(http.StatusMethodNotAllowed, r)
	}
}

func addWorkerMetadataHTTP(addr string, ct string, orig []byte) (data []byte, code int, err error) {
	ar := &pb.ActionResult{}
	if ct == "application/json" {
		err = jsonpb.Unmarshal(bytes.NewReader(orig), ar)
	} else {
		err = proto.Unmarshal(orig, ar)
	}
	if err != nil {
		return orig, http.StatusBadRequest, err
	}

	if ar.ExecutionMetadata == nil {
		ar.ExecutionMetadata = &pb.ExecutedActionMetadata{}
	} else if ar.ExecutionMetadata.Worker != "" {
		return orig, http.StatusOK, nil
	}

	worker := addr
	if worker == "" {
		worker, _, err = net.SplitHostPort(addr)
		if err != nil || worker == "" {
			worker = "unknown"
		}
	}

	ar.ExecutionMetadata.Worker = worker

	data, err = proto.Marshal(ar)
	if err != nil {
		return orig, http.StatusInternalServerError, err
	}

	return data, http.StatusOK, nil
}

// Produce a debugging page with some stats about the cache.
func (h *httpCache) StatusPageHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	totalSize, reservedSize, numItems := h.cache.Stats()

	goroutines := runtime.NumGoroutine()

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	enc.Encode(statusPageData{
		MaxSize:       h.cache.MaxSize(),
		CurrSize:      totalSize,
		ReservedSize:  reservedSize,
		NumFiles:      numItems,
		ServerTime:    time.Now().Unix(),
		GitCommit:     h.gitCommit,
		NumGoroutines: goroutines,
	})
}

func path(kind cache.EntryKind, hash string) string {
	return fmt.Sprintf("/%s/%s", kind, hash)
}
