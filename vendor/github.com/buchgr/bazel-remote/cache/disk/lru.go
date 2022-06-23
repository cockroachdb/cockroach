package disk

import (
	"container/list"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	gaugeCacheSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bazel_remote_disk_cache_size_bytes",
		Help: "The current number of bytes in the disk backend",
	})

	counterEvictedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_evicted_bytes_total",
		Help: "The total number of bytes evicted from disk backend, due to full cache",
	})

	counterOverwrittenBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_disk_cache_overwritten_bytes_total",
		Help: "The total number of bytes removed from disk backend, due to put of already existing key",
	})
)

type sizedItem interface {
	Size() int64
}

// Key is the type used for identifying cache items. For non-test code,
// this is a string of the form "<keyspace>/<hash>". Some test code uses
// ints for simplicity.
//
// TODO: update the test code to use strings too, then drop all the
// type assertions.
type Key interface{}

// EvictCallback is the type of callbacks that are invoked when items are evicted.
type EvictCallback func(key Key, value lruItem)

// SizedLRU is an LRU cache that will keep its total size below maxSize by evicting
// items.
// SizedLRU is not thread-safe.
type SizedLRU struct {
	// Eviction double-linked list. Most recently accessed elements are at the front.
	ll *list.List
	// Map to access the items in O(1) time
	cache map[interface{}]*list.Element

	// Includes reserved size.
	currentSize int64

	reservedSize int64

	// SizedLRU will evict items as needed to maintain the total size of the cache
	// below maxSize.
	maxSize int64

	onEvict EvictCallback
}

type entry struct {
	key   Key
	value lruItem
}

// NewSizedLRU returns a new SizedLRU cache
func NewSizedLRU(maxSize int64, onEvict EvictCallback) SizedLRU {
	return SizedLRU{
		maxSize: maxSize,
		ll:      list.New(),
		cache:   make(map[interface{}]*list.Element),
		onEvict: onEvict,
	}
}

// Add adds a (key, value) to the cache, evicting items as necessary. Add returns false (
// and does not add the item) if the item size is larger than the maximum size of the cache,
// or it cannot be added to the cache because too much space is reserved.
func (c *SizedLRU) Add(key Key, value lruItem) (ok bool) {
	if value.size > c.maxSize {
		return false
	}

	var sizeDelta int64
	if ee, ok := c.cache[key]; ok {
		sizeDelta = value.size - ee.Value.(*entry).value.size
		if c.reservedSize+sizeDelta > c.maxSize {
			return false
		}
		c.ll.MoveToFront(ee)
		counterOverwrittenBytes.Add(float64(ee.Value.(*entry).value.size))
		ee.Value.(*entry).value = value
	} else {
		sizeDelta = value.size
		if c.reservedSize+sizeDelta > c.maxSize {
			return false
		}
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
	}

	// Eviction. This is needed even if the key was already present, since the size of the
	// value might have changed, pushing the total size over maxSize.
	for c.currentSize+sizeDelta > c.maxSize {
		ele := c.ll.Back()
		if ele != nil {
			c.removeElement(ele)
		}
	}

	c.currentSize += sizeDelta

	gaugeCacheSizeBytes.Set(float64(c.currentSize))

	return true
}

// Get looks up a key in the cache
func (c *SizedLRU) Get(key Key) (value lruItem, ok bool) {
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}

	return
}

// Remove removes a (key, value) from the cache
func (c *SizedLRU) Remove(key Key) {
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
		gaugeCacheSizeBytes.Set(float64(c.currentSize))
	}
}

// Len returns the number of items in the cache
func (c *SizedLRU) Len() int {
	return len(c.cache)
}

func (c *SizedLRU) TotalSize() int64 {
	return c.currentSize
}

func (c *SizedLRU) ReservedSize() int64 {
	return c.reservedSize
}

func (c *SizedLRU) MaxSize() int64 {
	return c.maxSize
}

// This assumes that a is positive, b is non-negative, and c is positive.
func sumLargerThan(a, b, c int64) bool {
	sum := a + b
	if sum > c {
		return true
	}

	if sum <= 0 {
		// This indicates int64 overflow occurred.
		return true
	}

	return false
}

var errReservation = errors.New("internal reservation error")

func (c *SizedLRU) Reserve(size int64) (bool, error) {
	if size == 0 {
		return true, nil
	}

	if size < 0 || size > c.maxSize {
		return false, nil
	}

	if sumLargerThan(size, c.reservedSize, c.maxSize) {
		// If size + c.reservedSize is larger than c.maxSize
		// then we cannot evict enough items to make enough
		// space.
		return false, nil
	}

	// Evict elements until we are able to reserve enough space.
	for sumLargerThan(size, c.currentSize, c.maxSize) {
		ele := c.ll.Back()
		if ele != nil {
			c.removeElement(ele)
		} else {
			return false, errReservation // This should have been caught at the start.
		}
	}

	c.currentSize += size
	c.reservedSize += size
	return true, nil
}

func (c *SizedLRU) Unreserve(size int64) error {
	if size == 0 {
		return nil
	}

	if size < 0 {
		return fmt.Errorf("INTERNAL ERROR: should not try to unreserve negative value: %d", size)
	}

	newC := c.currentSize - size
	newR := c.reservedSize - size

	if newC < 0 || newR < 0 {
		return fmt.Errorf("INTERNAL ERROR: failed to unreserve: %d", size)
	}

	c.currentSize = newC
	c.reservedSize = newR

	return nil
}

func (c *SizedLRU) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	c.currentSize -= kv.value.size
	counterEvictedBytes.Add(float64(kv.value.size))

	if c.onEvict != nil {
		c.onEvict(kv.key, kv.value)
	}
}
