package disk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"

	"github.com/buchgr/bazel-remote/cache"
	"github.com/buchgr/bazel-remote/utils/tempfile"

	"github.com/djherbis/atime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pb "github.com/buchgr/bazel-remote/genproto/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
)

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_hits",
		Help: "The total number of disk backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_misses",
		Help: "The total number of disk backend cache misses",
	})
)

var tfc = tempfile.NewCreator()

// lruItem is the type of the values stored in SizedLRU to keep track of items.
type lruItem struct {
	size int64
}

// Cache is a filesystem-based LRU cache, with an optional backend proxy.
type Cache struct {
	dir   string
	proxy cache.Proxy

	mu  sync.Mutex
	lru SizedLRU
}

type nameAndInfo struct {
	name string // relative path
	info os.FileInfo
}

const sha256HashStrSize = sha256.Size * 2 // Two hex characters per byte.
const emptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// New returns a new instance of a filesystem-based cache rooted at `dir`,
// with a maximum size of `maxSizeBytes` bytes and an optional backend `proxy`.
// Cache is safe for concurrent use.
func New(dir string, maxSizeBytes int64, proxy cache.Proxy) (*Cache, error) {
	// Create the directory structure.
	hexLetters := []byte("0123456789abcdef")
	for _, c1 := range hexLetters {
		for _, c2 := range hexLetters {
			subDir := string(c1) + string(c2)
			err := os.MkdirAll(filepath.Join(dir, cache.CAS.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.AC.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.RAW.String(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}

	// The eviction callback deletes the file from disk.
	// This function is only called while the lock is held
	// by the current goroutine.
	onEvict := func(key Key, value lruItem) {
		f := filepath.Join(dir, key.(string))
		err := os.Remove(f)
		if err != nil {
			log.Printf("ERROR: failed to remove evicted cache file: %s", f)
		}
	}

	c := &Cache{
		dir:   filepath.Clean(dir),
		proxy: proxy,
		lru:   NewSizedLRU(maxSizeBytes, onEvict),
	}

	err := c.migrateDirectories()
	if err != nil {
		return nil, fmt.Errorf("Attempting to migrate the old directory structure failed: %w", err)
	}
	err = c.loadExistingFiles()
	if err != nil {
		return nil, fmt.Errorf("Loading of existing cache entries failed due to error: %w", err)
	}

	return c, nil
}

func (c *Cache) migrateDirectories() error {
	err := migrateDirectory(filepath.Join(c.dir, cache.AC.String()))
	if err != nil {
		return err
	}
	err = migrateDirectory(filepath.Join(c.dir, cache.CAS.String()))
	if err != nil {
		return err
	}
	// Note: there are no old "RAW" directories (yet).
	return nil
}

func migrateDirectory(dir string) error {
	log.Printf("Migrating files (if any) to new directory structure: %s\n", dir)

	listing, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	// The v0 directory structure was lowercase sha256 hash filenames
	// stored directly in the ac/ and cas/ directories.
	hashKeyRegex := regexp.MustCompile("^[a-f0-9]{64}$")

	// The v1 directory structure has subdirs for each two lowercase
	// hex character pairs.
	v1DirRegex := regexp.MustCompile("^[a-f0-9]{2}$")

	for _, item := range listing {
		oldName := item.Name()
		oldNamePath := filepath.Join(dir, oldName)

		if item.IsDir() {
			if !v1DirRegex.MatchString(oldName) {
				// Warn about non-v1 subdirectories.
				log.Println("Warning: unexpected directory", oldNamePath)
			}
			continue
		}

		if !item.Mode().IsRegular() {
			log.Println("Warning: skipping non-regular file:", oldNamePath)
			continue
		}

		if !hashKeyRegex.MatchString(oldName) {
			log.Println("Warning: skipping unexpected file:", oldNamePath)
			continue
		}

		newName := filepath.Join(dir, oldName[:2], oldName)
		err = os.Rename(filepath.Join(dir, oldName), newName)
		if err != nil {
			return err
		}
	}

	return nil
}

// loadExistingFiles lists all files in the cache directory, and adds them to the
// LRU index so that they can be served. Files are sorted by access time first,
// so that the eviction behavior is preserved across server restarts.
func (c *Cache) loadExistingFiles() error {
	log.Printf("Loading existing files in %s.\n", c.dir)

	// Walk the directory tree
	var files []nameAndInfo
	err := filepath.Walk(c.dir, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error while walking directory:", err)
			return err
		}

		if !info.IsDir() {
			files = append(files, nameAndInfo{name: name, info: info})
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Println("Sorting cache files by atime.")
	// Sort in increasing order of atime
	sort.Slice(files, func(i int, j int) bool {
		return atime.Get(files[i].info).Before(atime.Get(files[j].info))
	})

	log.Println("Building LRU index.")
	for _, f := range files {
		relPath := f.name[len(c.dir)+1:]
		ok := c.lru.Add(relPath, lruItem{size: f.info.Size()})
		if !ok {
			err = os.Remove(filepath.Join(c.dir, relPath))
			if err != nil {
				return err
			}
		}
	}

	log.Println("Finished loading disk cache files.")
	return nil
}

// Put stores a stream of `size` bytes from `r` into the cache.
// If `hash` is not the empty string, and the contents don't match it,
// a non-nil error is returned. All data will be read from `r` before
// this function returns.
func (c *Cache) Put(kind cache.EntryKind, hash string, size int64, r io.Reader) (rErr error) {
	defer func() {
		if r != nil {
			_, _ = io.Copy(ioutil.Discard, r)
		}
	}()

	if size < 0 {
		return fmt.Errorf("Invalid (negative) size: %d", size)
	}

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return fmt.Errorf("Invalid hash size: %d, expected: %d",
			len(hash), sha256.Size)
	}

	if kind == cache.CAS && size == 0 && hash == emptySha256 {
		return nil
	}

	key := cache.Key(kind, hash)

	var tf *os.File // Tempfile.

	// Cleanup intermediate state if something went wrong and we
	// did not successfully commit.
	unreserve := false
	removeTempfile := false
	defer func() {
		// No lock required to remove stray tempfiles.
		if removeTempfile {
			os.Remove(tf.Name())
		}

		if unreserve {
			c.mu.Lock()
			err := c.lru.Unreserve(size)
			if err != nil {
				// Set named return value.
				rErr = err
				log.Printf(rErr.Error())
			}
			c.mu.Unlock()
		}
	}()

	if size > 0 {
		c.mu.Lock()
		ok, err := c.lru.Reserve(size)
		if err != nil {
			c.mu.Unlock()
			return &cache.Error{
				Code: http.StatusInternalServerError,
				Text: err.Error(),
			}
		}
		if !ok {
			c.mu.Unlock()
			return &cache.Error{
				Code: http.StatusInsufficientStorage,
				Text: fmt.Sprintf("The item (%d) + reserved space is larger than the cache's maximum size (%d).",
					size, c.lru.MaxSize()),
			}
		}
		c.mu.Unlock()
		unreserve = true
	}

	// Final destination, if all goes well.
	filePath := cacheFilePath(kind, c.dir, hash)

	// We will download to this temporary file.
	tf, err := tfc.Create(filePath)
	if err != nil {
		return err
	}
	removeTempfile = true

	err = writeAndCloseFile(r, kind, hash, size, tf)
	if err != nil {
		return err
	}

	r = nil // We read all the data from r.

	if c.proxy != nil {
		rc, err := os.Open(tf.Name())
		if err != nil {
			log.Println("Failed to proxy Put:", err)
		} else {
			// Doesn't block, should be fast.
			c.proxy.Put(kind, hash, size, rc)
		}
	}

	unreserve, removeTempfile, err = c.commit(key, tf.Name(), filePath, size, size)

	// Might be nil.
	return err
}

func writeAndCloseFile(r io.Reader, kind cache.EntryKind, hash string, size int64, f *os.File) error {
	closeFile := true
	defer func() {
		if closeFile {
			f.Close()
		}
	}()

	var err error

	var bytesCopied int64
	if kind == cache.CAS {
		hasher := sha256.New()
		bytesCopied, err = io.Copy(io.MultiWriter(f, hasher), r)
		if err != nil {
			return err
		}
		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != hash {
			return fmt.Errorf(
				"checksums don't match. Expected %s, found %s", hash, actualHash)
		}
	} else {
		if bytesCopied, err = io.Copy(f, r); err != nil {
			return err
		}
	}

	if isSizeMismatch(bytesCopied, size) {
		return fmt.Errorf(
			"sizes don't match. Expected %d, found %d", size, bytesCopied)
	}

	if err = f.Sync(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}
	closeFile = false

	return nil
}

// This must be called when the lock is not held.
func (c *Cache) commit(key string, tempfile string, finalPath string, reservedSize int64, foundSize int64) (unreserve bool, removeTempfile bool, err error) {
	unreserve = reservedSize > 0
	removeTempfile = true

	c.mu.Lock()
	defer c.mu.Unlock()

	if unreserve {
		err = c.lru.Unreserve(reservedSize)
		if err != nil {
			log.Println(err.Error())
			return true, removeTempfile, err
		}
	}
	unreserve = false

	if !c.lru.Add(key, lruItem{size: foundSize}) {
		err = fmt.Errorf("INTERNAL ERROR: failed to add: %s, size %d", key, foundSize)
		log.Println(err.Error())
		return unreserve, removeTempfile, err
	}

	err = syscall.Rename(tempfile, finalPath)
	if err != nil {
		log.Printf("INTERNAL ERROR: failed to rename \"%s\" to \"%s\": %v",
			tempfile, finalPath, err)
		log.Println("Removing", key)
		c.lru.Remove(key)
		return unreserve, removeTempfile, err
	}
	removeTempfile = false

	// Commit successful if we made it this far! \o/
	return unreserve, removeTempfile, nil
}

// Return a non-nil io.ReadCloser and non-negative size if the item is available
// locally, and a boolean that indicates if the item is not available locally
// but that we can try the proxy backend.
func (c *Cache) availableOrTryProxy(key string, size int64, blobPath string) (rc io.ReadCloser, foundSize int64, tryProxy bool, err error) {
	locked := true
	c.mu.Lock()

	item, available := c.lru.Get(key)
	if available {
		c.mu.Unlock() // We expect a cache hit below.
		locked = false

		if !isSizeMismatch(size, item.size) {
			var f *os.File
			f, err = os.Open(blobPath)
			if err != nil {
				// Race condition, was the item purged after we released the lock?
				log.Printf("Warning: expected %s to exist on disk, undersized cache?", blobPath)
			} else {
				var fileInfo os.FileInfo
				fileInfo, err = f.Stat()
				foundSize := fileInfo.Size()
				if isSizeMismatch(size, foundSize) {
					// Race condition, was the item replaced after we released the lock?
					log.Printf("Warning: expected %s to on disk to have size %d, found %d",
						blobPath, size, foundSize)
				} else {
					return f, foundSize, false, nil
				}
			}
		}
	}
	err = nil

	if c.proxy != nil {
		if size > 0 {
			// If we know the size, attempt to reserve that much space.
			if !locked {
				c.mu.Lock()
			}
			tryProxy, err = c.lru.Reserve(size)
			c.mu.Unlock()
			locked = false
		} else {
			// If the size is unknown, take a risk and hope it's not
			// too large.
			tryProxy = true
		}
	}

	if locked {
		c.mu.Unlock()
	}

	return nil, -1, tryProxy, err
}

// Get returns an io.ReadCloser with the content of the cache item stored
// under `hash` and the number of bytes that can be read from it. If the
// item is not found, the io.ReadCloser will be nil. If some error occurred
// when processing the request, then it is returned. Callers should provide
// the `size` of the item to be retrieved, or -1 if unknown.
func (c *Cache) Get(kind cache.EntryKind, hash string, size int64) (rc io.ReadCloser, s int64, rErr error) {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return nil, -1, fmt.Errorf("Invalid hash size: %d, expected: %d",
			len(hash), sha256.Size)
	}

	if kind == cache.CAS && size <= 0 && hash == emptySha256 {
		cacheHits.Inc()
		return ioutil.NopCloser(bytes.NewReader([]byte{})), 0, nil
	}

	var err error
	key := cache.Key(kind, hash)

	var tf *os.File // Tempfile we will write to.

	// Cleanup intermediate state if something went wrong and we
	// did not successfully commit.
	unreserve := false
	removeTempfile := false
	defer func() {
		// No lock required to remove stray tempfiles.
		if removeTempfile {
			os.Remove(tf.Name())
		}

		if unreserve {
			c.mu.Lock()
			err := c.lru.Unreserve(size)
			if err != nil {
				// Set named return value.
				rErr = err
				log.Printf(rErr.Error())
			}
			c.mu.Unlock()
		}
	}()

	blobPath := cacheFilePath(kind, c.dir, hash)
	f, foundSize, tryProxy, err := c.availableOrTryProxy(key, size, blobPath)
	if err != nil {
		return nil, -1, err
	}
	if tryProxy && size > 0 {
		unreserve = true
	}
	if f != nil {
		cacheHits.Inc()
		return f, foundSize, nil
	}

	cacheMisses.Inc()

	if !tryProxy {
		return nil, -1, nil
	}

	r, foundSize, err := c.proxy.Get(kind, hash)
	if r != nil {
		defer r.Close()
	}
	if err != nil || r == nil {
		return nil, -1, err
	}
	if isSizeMismatch(size, foundSize) {
		return nil, -1, nil
	}

	tf, err = tfc.Create(blobPath)
	if err != nil {
		return nil, -1, err
	}
	removeTempfile = true

	err = writeAndCloseFile(r, kind, hash, foundSize, tf)
	if err != nil {
		return nil, -1, err
	}

	rc, err = os.Open(tf.Name())
	if err != nil {
		return nil, -1, err
	}

	unreserve, removeTempfile, err = c.commit(key, tf.Name(), blobPath, size, foundSize)
	if err != nil {
		rc.Close()
		rc = nil
		foundSize = -1
	}

	return rc, foundSize, err
}

// Contains returns true if the `hash` key exists in the cache, and
// the size if known (or -1 if unknown).
//
// If there is a local cache miss, the proxy backend (if there is
// one) will be checked.
//
// Callers should provide the `size` of the item, or -1 if unknown.
func (c *Cache) Contains(kind cache.EntryKind, hash string, size int64) (bool, int64) {

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return false, -1
	}

	if kind == cache.CAS && size <= 0 && hash == emptySha256 {
		return true, 0
	}

	foundSize := int64(-1)
	key := cache.Key(kind, hash)

	c.mu.Lock()
	item, exists := c.lru.Get(key)
	if exists {
		foundSize = item.size
	}
	c.mu.Unlock()

	if exists && !isSizeMismatch(size, foundSize) {
		return true, foundSize
	}

	if c.proxy != nil {
		exists, foundSize = c.proxy.Contains(kind, hash)
		if exists && !isSizeMismatch(size, foundSize) {
			return true, foundSize
		}
	}

	return false, -1
}

// MaxSize returns the maximum cache size in bytes.
func (c *Cache) MaxSize() int64 {
	// The underlying value is never modified, no need to lock.
	return c.lru.MaxSize()
}

// Stats returns the current size of the cache in bytes, and the number of
// items stored in the cache.
func (c *Cache) Stats() (totalSize int64, reservedSize int64, numItems int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lru.TotalSize(), c.lru.ReservedSize(), c.lru.Len()
}

func isSizeMismatch(requestedSize int64, foundSize int64) bool {
	return requestedSize > -1 && foundSize > -1 && requestedSize != foundSize
}

func ensureDirExists(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func cacheFilePath(kind cache.EntryKind, cacheDir string, hash string) string {
	return filepath.Join(cacheDir, cache.Key(kind, hash))
}

// GetValidatedActionResult returns a valid ActionResult and its serialized
// value from the CAS if it and all its dependencies are also available. If
// not, nil values are returned. If something unexpected went wrong, return
// an error.
func (c *Cache) GetValidatedActionResult(hash string) (*pb.ActionResult, []byte, error) {

	rc, sizeBytes, err := c.Get(cache.AC, hash, -1)
	if rc != nil {
		defer rc.Close()
	}
	if err != nil {
		return nil, nil, err
	}

	if rc == nil || sizeBytes <= 0 {
		return nil, nil, nil // aka "not found"
	}

	acdata, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, nil, err
	}

	result := &pb.ActionResult{}
	err = proto.Unmarshal(acdata, result)
	if err != nil {
		return nil, nil, err
	}

	for _, f := range result.OutputFiles {
		if len(f.Contents) == 0 {
			found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}
	}

	for _, d := range result.OutputDirectories {
		r, size, err := c.Get(cache.CAS, d.TreeDigest.Hash, d.TreeDigest.SizeBytes)
		if r == nil {
			return nil, nil, err // aka "not found", or an err if non-nil
		}
		if err != nil {
			r.Close()
			return nil, nil, err
		}
		if size != d.TreeDigest.SizeBytes {
			r.Close()
			return nil, nil, fmt.Errorf("expected %d bytes, found %d",
				d.TreeDigest.SizeBytes, size)
		}

		var oddata []byte
		oddata, err = ioutil.ReadAll(r)
		r.Close()
		if err != nil {
			return nil, nil, err
		}

		tree := pb.Tree{}
		err = proto.Unmarshal(oddata, &tree)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range tree.Root.GetFiles() {
			if f.Digest == nil {
				continue
			}
			found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
			if !found {
				return nil, nil, nil // aka "not found"
			}
		}

		for _, child := range tree.GetChildren() {
			for _, f := range child.GetFiles() {
				if f.Digest == nil {
					continue
				}
				found, _ := c.Contains(cache.CAS, f.Digest.Hash, f.Digest.SizeBytes)
				if !found {
					return nil, nil, nil // aka "not found"
				}
			}
		}
	}

	if result.StdoutDigest != nil {
		found, _ := c.Contains(cache.CAS, result.StdoutDigest.Hash, result.StdoutDigest.SizeBytes)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	if result.StderrDigest != nil {
		found, _ := c.Contains(cache.CAS, result.StderrDigest.Hash, result.StderrDigest.SizeBytes)
		if !found {
			return nil, nil, nil // aka "not found"
		}
	}

	return result, acdata, nil
}
