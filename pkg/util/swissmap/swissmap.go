// package swissmap is a Go implementation of Swiss Tables as described in
// https://abseil.io/about/design/swisstables. See also:
// https://faultlore.com/blah/hashbrown-tldr/.
//
// Google's C++ implementation:
//
//	https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h
//
// # Swiss Tables
//
// Swiss tables are hash tables that map keys to values, similar to Go's
// builtin map type. Swiss tables use open-addressing rather than chaining to
// handle collisions. If you're not familiar with open-addressing see
// https://en.wikipedia.org/wiki/Open_addressing. A hybrid between linear and
// quadratic probing is used - linear probing within groups of small fixed
// size and quadratic probing at the group level. The key design choice of
// Swiss tables is the usage of a separate metadata array that stores 1 byte
// per slot in the table. 7-bits of this "control byte" are taken from
// hash(key) and the remaining bit is used to indicate whether the slot is
// empty, full, deleted, or a sentinel. The metadata array allows quick
// probes. The Google implementation of Swiss tables uses SIMD on x86 CPUs in
// order to quickly check 16 slots at a time for a match. Neon on arm64 CPUs
// is apparently too high latency, but the generic version is still able to
// compare 8 bytes at time through bit tricks (SWAR, SIMD Within A Register).
//
// A Swiss table's layout is N-1 slots where N is a power of 2 and N+groupSize
// control bytes. The [N:N+groupSize] control bytes mirror the first groupSize
// control bytes so that probe operations at the end of the control bytes
// array do not have to perform additional checks. The control byte for slot N
// is always a sentinel which is considered empty for the purposes of probing
// but is not available for storing an entry and is also not a deletion
// tombstone.
//
// Probing is done by taking the top 57 bits of hash(key)%N as the index into
// the control bytes and then performing a check of the groupSize control
// bytes at that index. Note that these groups are not aligned on a groupSize
// boundary (i.e. groups are conceptual, not physical, and they overlap) and
// an unaligned memory access is performed. According to
// https://lemire.me/blog/2012/05/31/data-alignment-for-speed-myth-or-reality/,
// data alignment for performance is a myth on modern CPUs. Probing walks
// through groups in the table using quadratic probing until it finds a group
// that has at least one empty slot or the sentinel control byte. See the
// comments on probeSeq for more details on the order in which groups are
// probed and the guarantee that every group is examined which means that in
// the worst case probing will end when the sentinel is encountered.
//
// Deletion is performed using tombstones (ctrlDeleted) with an optimization
// to mark a slot as empty if we can prove that doing so would not violate the
// probing behavior that a group of full slots causes probing to continue. It
// is invalid to take a group of full slots and mark one as empty as doing so
// would cause subsequent lookups to terminate at that group rather than
// continue to probe. We prove a slot was never part of a full group by
// looking for whether any of the groupSize-1 neighbors to the left and right
// of the deleting slot are empty which indicates that the slot was never part
// of a full group.
//
// # Implementation
//
// The implementation follows Google's Abseil implementation of Swiss Tables,
// and is heavily tuned, using unsafe and raw pointer arithmentic rather than
// Go slices to squeeze out every drop of performance. In order to support
// hashing of arbitrary keys, a hack is performed to extract the hash function
// from Go's implementation of map[K]struct{} by reaching into the internals
// of the type. (This might break in future version of Go, but is likely
// fixable unless the Go runtime does something drastic).
//
// # Performance
//
// A swissmap has similar or slightly better performance than Go's builtin map
// for small map sizes, and is much faster at large map sizes (old=go-map,
// new=swissmap):
//
//	name                        old time/op  new time/op  delta
//	StringMaps/n=16/map-10      7.19ns ± 3%  7.28ns ± 0%     ~     (p=0.154 n=9+9)
//	StringMaps/n=128/map-10     7.66ns ± 5%  7.37ns ± 3%   -3.74%  (p=0.008 n=10+9)
//	StringMaps/n=1024/map-10    10.8ns ± 3%   7.6ns ± 3%  -29.76%  (p=0.000 n=10+10)
//	StringMaps/n=8192/map-10    20.3ns ± 2%   7.9ns ± 1%  -61.16%  (p=0.000 n=10+10)
//	StringMaps/n=131072/map-10  26.1ns ± 0%  14.0ns ± 1%  -46.56%  (p=0.000 n=10+10)
//	Int64Maps/n=16/map-10       4.96ns ± 1%  4.83ns ± 0%   -2.73%  (p=0.000 n=9+9)
//	Int64Maps/n=128/map-10      5.19ns ± 3%  4.89ns ± 5%   -5.80%  (p=0.000 n=10+10)
//	Int64Maps/n=1024/map-10     6.80ns ± 5%  5.01ns ± 2%  -26.32%  (p=0.000 n=10+10)
//	Int64Maps/n=8192/map-10     17.4ns ± 1%   5.3ns ± 0%  -69.59%  (p=0.000 n=10+7)
//	Int64Maps/n=131072/map-10   20.6ns ± 0%   6.7ns ± 0%  -67.67%  (p=0.000 n=10+9)
//
// A swissmap dominates the performance of the RobinHood map used by Pebble's
// block-cache (old=robinhood, new=swissmap):
//
//	name                        old time/op  new time/op  delta
//	StringMaps/n=16/map-10      11.7ns ±28%   7.3ns ± 0%  -37.68%  (p=0.000 n=10+9)
//	StringMaps/n=128/map-10     12.6ns ± 5%   7.4ns ± 3%  -41.44%  (p=0.000 n=9+9)
//	StringMaps/n=1024/map-10    14.1ns ± 7%   7.6ns ± 3%  -46.30%  (p=0.000 n=10+10)
//	StringMaps/n=8192/map-10    17.7ns ± 4%   7.9ns ± 1%  -55.39%  (p=0.000 n=10+10)
//	StringMaps/n=131072/map-10  25.5ns ± 1%  14.0ns ± 1%  -45.20%  (p=0.000 n=10+10)
//	Int64Maps/n=16/map-10       4.96ns ± 7%  4.83ns ± 0%   -2.72%  (p=0.012 n=10+9)
//	Int64Maps/n=128/map-10      4.92ns ± 4%  4.89ns ± 5%     ~     (p=0.085 n=10+10)
//	Int64Maps/n=1024/map-10     5.63ns ± 5%  5.01ns ± 2%  -11.02%  (p=0.000 n=10+10)
//	Int64Maps/n=8192/map-10     11.1ns ± 4%   5.3ns ± 0%  -52.46%  (p=0.000 n=10+7)
//	Int64Maps/n=131072/map-10   14.3ns ± 1%   6.7ns ± 0%  -53.33%  (p=0.000 n=10+9)
//
// # Caveats
//
//   - Resizing for a swissmap is done for the whole table rather than the
//     incremental resizing performed by Go's builtin map. This is pretty
//     fundamental to the usage of open-addressing.
//
// TODO(peter):
//   - Add correctness tests.
//   - Add All method.
//   - Add support for rehash in-place.
//   - Add support for SIMD searching on x86.
//   - Add support for 8-byte Neon SIMD searching:
//     https://community.arm.com/arm-community-blogs/b/infrastructure-solutions-blog/posts/porting-x86-vector-bitmask-optimizations-to-arm-neon
//     https://github.com/abseil/abseil-cpp/commit/6481443560a92d0a3a55a31807de0cd712cd4f88
//   - Abstract out the slice allocations so we can use manual memory allocation
//     when used inside Pebble.
//   - Benchmark insertion and deletion.
//   - Add a note on thread safety (there isn't any).
//   - Add a note that a little endian system is required, and a test that asserts that.
package swissmap

import (
	"fmt"
	"math/bits"
	"strings"
	"unsafe"
)

const (
	debug = false

	groupSize       = 8
	maxAvgGroupLoad = 7

	ctrlEmpty    ctrl = 0b10000000
	ctrlDeleted  ctrl = 0b11111110
	ctrlSentinel ctrl = 0b11111111

	bitsetLSB     = 0x0101010101010101
	bitsetMSB     = 0x8080808080808080
	bitsetEmpty   = bitsetLSB * uint64(ctrlEmpty)
	bitsetDeleted = bitsetLSB * uint64(ctrlDeleted)
)

// slot holds a key and value.
type slot[K comparable, V any] struct {
	key   K
	value V
}

// M is an unordered map from keys to values with Put, Get, Delete, and All
// operations. It is inspired by Google's Swiss Tables design as implemented
// in Abseil's flat_hash_map.
type M[K comparable, V any] struct {
	// ctrls is capacity+groupSize in length. Ctrls[capacity] is always
	// ctrlSentinel which is used to stop probe iteration. A copy of the first
	// groupSize-1 elements of ctrls is mirrored into the remaining slots
	// which is done so that a probe sequence which picks a value near the end
	// of ctrls will have valid control bytes to look at.
	//
	// When the map is empty, ctrls points to emptyCtrls which will never be
	// modified and is used to simplify the Put, Get, and Delete code which
	// doesn't have to check for a nil ctrls.
	ctrls unsafeSlice[ctrl]
	// slots is capacity in length.
	slots unsafeSlice[slot[K, V]]
	// The hash function to each keys of type K. The hash function is
	// extracted from the Go runtime's implementation of map[K]struct{}.
	hash func(key unsafe.Pointer, seed uintptr) uintptr
	seed uintptr
	// The total number slots (always 2^N-1). The capacity is used as a mask
	// to quickly compute i%N using a bitwise & operation.
	capacity uintptr
	// The number of filled slots (i.e. the number of elements in the map).
	used int
	// The number of slots we can still fill without needing to rehash.
	//
	// This is stored separately due to tombstones: we do not include
	// tombstones in the growth capacity because we'd like to rehash when the
	// table is filled with tombstones as otherwise probe sequences might get
	// unacceptably long without triggering a rehash.
	growthLeft int
}

// New constructs a new M with the specified initial capacity. If
// initialCapacity is 0 the map will start out with zero capacity and will
// grow on the first insert. The zero value for an M is not usable.
func New[K comparable, V any](initialCapacity int) *M[K, V] {
	m := &M[K, V]{
		ctrls: emptyCtrls,
		hash:  getRuntimeHasher[K](),
		seed:  uintptr(fastrand64()),
	}
	if initialCapacity > 0 {
		targetCapacity := (1 << (uint(bits.Len(uint(2*initialCapacity-1))) - 1)) - 1
		m.rehash(uintptr(targetCapacity))
	}
	return m
}

// Put inserts an entry into the map, overwriting an existing value if an
// entry with the same key already exists.
func (m *M[K, V]) Put(key K, value V) {
	// Put is find composed with uncheckedPut. We perform find to see if the
	// key is already present. If it is, we're done and overwrite the existing
	// value. If the value isn't present we perform an uncheckedPut which
	// inserts an entry known not to be in the table (violating this
	// requirement will cause the table to behave erratically).
	h := m.hash(noescape(unsafe.Pointer(&key)), m.seed)

	// NB: Unlike the abseil swiss table implementation which uses a common
	// find routine for Get, Put, and Delete, we have to manually inline the
	// find routine for performance.
	seq := makeProbeSeq(h1(h), m.capacity)
	if debug {
		fmt.Printf("put(%v): %s\n", key, seq)
	}

	for ; ; seq = seq.next() {
		g := m.ctrls.At(seq.offset)
		match := g.matchH2(h2(h))
		if debug {
			fmt.Printf("put(probing): offset=%d h2=%02x match=%s [% 02x]\n",
				seq.offset, h2(h), match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}

		for match != 0 {
			bit := match.next()
			i := seq.offsetAt(bit)
			if debug {
				fmt.Printf("put(checking): index=%d  key=%v\n", i, m.slots.At(i).key)
			}
			slot := m.slots.At(i)
			if key == slot.key {
				if debug {
					fmt.Printf("put(updating): index=%d  key=%v\n", i, key)
				}
				slot.value = value
				return
			}
			match = match.clear(bit)
		}

		match = g.matchEmpty()
		if match != 0 {
			if debug {
				fmt.Printf("put(not-found): offset=%d match-empty=%s [% 02x]\n",
					seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
			}
			m.uncheckedPut(h, key, value)
			m.used++
			return
		}

		if debug {
			fmt.Printf("put(skipping): offset=%d match-empty=%s [% 02x]\n",
				seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}
	}
}

// Get retrieves the value from the map for the specified key, return ok=false
// if the key is not present.
func (m *M[K, V]) Get(key K) (value V, ok bool) {
	h := m.hash(noescape(unsafe.Pointer(&key)), m.seed)

	// NB: Unlike the abseil swiss table implementation which uses a common
	// find routine for Get, Put, and Delete, we have to manually inline the
	// find routine for performance.

	// To find the location of a key in the table, we compute hash(key). From
	// h1(hash(key)) and the capacity, we construct a probeSeq that visits every
	// group of slots in some interesting order.
	//
	// We walk through these indices. At each index, we select the entire group
	// starting with that index and extract potential candidates: occupied slots
	// with a control byte equal to h2(hash(key)). If we find an empty slot in the
	// group, we stop and return an error. The key at candidate slot y is compared
	// with key; if key == m.slots[y].key we are done and return y; otherwise we
	// continue to the next probe index. Tombstones (ctrlDeleted) effectively
	// behave like full slots that never match the value we're looking for.
	//
	// The h2 bits ensure when we compare a key we are likely to have actually
	// found the object. That is, the chance is low that keys compare false. Thus,
	// when we search for an object, we are unlikely to call == many times. This
	// likelyhood can be analyzed as follows (assuming that h2 is a random enough
	// hash function).
	//
	// Let's assume that there are k "wrong" objects that must be examined in a
	// probe sequence. For example, when doing a find on an object that is in the
	// table, k is the number of objects between the start of the probe sequence
	// and the final found object (not including the final found object). The
	// expected number of objects with an h2 match is then k/128. Measurements and
	// analysis indicate that even at high load factors, k is less than 32,
	// meaning that the number of false positive comparisons we must perform is
	// less than 1/8 per find.
	seq := makeProbeSeq(h1(h), m.capacity)
	if debug {
		fmt.Printf("get(%v): %s\n", key, seq)
	}

	for ; ; seq = seq.next() {
		g := m.ctrls.At(seq.offset)
		match := g.matchH2(h2(h))
		if debug {
			fmt.Printf("get(probing): offset=%d h2=%02x match=%s [% 02x]\n",
				seq.offset, h2(h), match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}

		for match != 0 {
			bit := match.next()
			i := seq.offsetAt(bit)
			if debug {
				fmt.Printf("get(checking): index=%d  key=%v\n", i, m.slots.At(i).key)
			}
			slot := m.slots.At(i)
			if key == slot.key {
				return slot.value, true
			}
			match = match.clear(bit)
		}

		match = g.matchEmpty()
		if match != 0 {
			if debug {
				fmt.Printf("get(not-found): offset=%d match-empty=%s [% 02x]\n",
					seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
			}
			return value, false
		}

		if debug {
			fmt.Printf("get(skipping): offset=%d match-empty=%s [% 02x]\n",
				seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}
	}
}

// Delete deletes the entry corresponding to the specified key from the map.
// It is a noop to delete a non-existent key.
func (m *M[K, V]) Delete(key K) {
	// Delete is find composed with "deleted at": we perform find(key), and
	// then delete at the resulting slot if found.
	h := m.hash(noescape(unsafe.Pointer(&key)), m.seed)

	// NB: Unlike the abseil swiss table implementation which uses a common
	// find routine for Get, Put, and Delete, we have to manually inline the
	// find routine for performance.
	seq := makeProbeSeq(h1(h), m.capacity)
	if debug {
		fmt.Printf("delete(%v): %s\n", key, seq)
	}

	for ; ; seq = seq.next() {
		g := m.ctrls.At(seq.offset)
		match := g.matchH2(h2(h))
		if debug {
			fmt.Printf("delete(probing): offset=%d h2=%02x match=%s [% 02x]\n",
				seq.offset, h2(h), match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}

		for match != 0 {
			bit := match.next()
			i := seq.offsetAt(bit)
			if debug {
				fmt.Printf("delete(checking): index=%d  key=%v\n", i, m.slots.At(i).key)
			}
			s := m.slots.At(i)
			if key == s.key {
				m.used--
				*s = slot[K, V]{}

				// Given an offset to delete we simply create a tombstone and
				// destroy its contents and mark the ctrl as deleted. If we
				// can prove that the slot would not appear in a probe
				// sequence we can mark the slot as empty instead. We can
				// prove this by checking to see if the slot is part of any
				// group that could have been full (assuming we never create
				// an empty slot in a group with no empties which this
				// heuristic guarantees we never do). If the slot is always
				// parts of groups that could never have been full then find
				// would stop at this slot since we do not probe beyond groups
				// with empties.
				if m.wasNeverFull(i) {
					m.setCtrl(i, ctrlEmpty)
					m.growthLeft++

					if debug {
						fmt.Printf("delete(%v): index=%d used=%d growth-left=%d\n",
							key, i, m.used, m.growthLeft)
					}
				} else {
					m.setCtrl(i, ctrlDeleted)

					if debug {
						fmt.Printf("delete(%v): index=%d used=%d\n", key, i, m.used)
					}
				}
				return
			}
			match = match.clear(bit)
		}

		match = g.matchEmpty()
		if match != 0 {
			if debug {
				fmt.Printf("delete(not-found): offset=%d match-empty=%s [% 02x]\n",
					seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
			}
			return
		}

		if debug {
			fmt.Printf("delete(skipping): offset=%d match-empty=%s [% 02x]\n",
				seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}
	}
}

// All calls f sequentially for each key and value present in the map. If f
// returns false, range stops the iteration.
//
// TODO(peter): The naming of All and its signature are meant to conform to
// the range-over-function Go proposal. When that proposal is accepted (which
// seems likely), we'll be able to iterate over the map by doing:
//
//	for k, v := range m.All {
//	  fmt.Printf("%v: %v\n", k, v)
//	}
//
// See https://github.com/golang/go/issues/61897.
func (m *M[K, V]) All(yield func(key K, value V) bool) {
	for i := uintptr(0); i < m.capacity; i++ {
		// Match full entries which have a high-bit of zero.
		if (*m.ctrls.At(i) & ctrlEmpty) != ctrlEmpty {
			s := m.slots.At(i)
			if !yield(s.key, s.value) {
				return
			}
		}
	}
}

// Len returns the number of entries in the map.
func (m *M[K, V]) Len() int {
	return m.used
}

// setCtrl sets the control byte at index i, taking care to mirror the byte to
// the end of the control bytes slice if i<groupSize.
func (m *M[K, V]) setCtrl(i uintptr, v ctrl) {
	*m.ctrls.At(i) = v
	// Mirror the first groupSize control state to the end of the ctrls slice.
	// We do this unconditionally which is faster than performing a comparison
	// to do it only for the first groupSize slots. Note that the index will
	// be the identity for slots in the range [groupSize,capacity).
	*m.ctrls.At(((i - (groupSize - 1)) & m.capacity) + (groupSize - 1)) = v
}

// wasNeverFull returns true if index i was never part a full group. This
// check allows an optimization during deletion whereby a deleted slot can be
// converted to empty rather than a tombstone. See the comment in Delete for
// further explanation.
func (m *M[K, V]) wasNeverFull(i uintptr) bool {
	if m.capacity < groupSize {
		// The map fits entirely in a single group so we will never probe
		// beyond this group.
		return true
	}

	indexBefore := (i - groupSize) & m.capacity
	emptyAfter := m.ctrls.At(i).matchEmpty()
	emptyBefore := m.ctrls.At(indexBefore).matchEmpty()
	// We count how many consecutive non empties we have to the right and to
	// the left of i. If the sum is >= groupSize then there is at least one
	// probe window that might have seen a full group.
	if emptyBefore != 0 && emptyAfter != 0 &&
		(emptyBefore.count()+emptyAfter.count()) < groupSize {
		return true
	}
	return false
}

// uncheckedPut inserts an entry known not to be in the table. Used by Put
// after it has failed to find an existing entry to overwrite duration
// insertion.
func (m *M[K, V]) uncheckedPut(h uintptr, key K, value V) {
	// Before performing the insertion we may decide the table is getting
	// overcrowded (i.e. the load factor is greater than 7/8 for big tables;
	// small tables use a max load factor of 1).
	if m.growthLeft == 0 {
		// TODO(peter): We only have to rehash if the slot we're trying to
		// insert into isn't deleted. Abseil handles this by first finding the
		// slot to insert into and only resizing if that slot is not deleted.
		// After resizing it has to re-find the slot to insert into, though
		// the table should be <50% empty so the first group it checks should
		// have an empty slot.
		m.rehash(2*m.capacity + 1)
	}

	// Given key and its hash hash(key), to insert it, we construct a
	// probeSeq, and use it to find the first group with an unoccupied (empty
	// or deleted) slot. We place the key/value into the first such slot in
	// the group and mark it as full with key's H2.
	seq := makeProbeSeq(h1(h), m.capacity)
	if debug {
		fmt.Printf("put(%v,%v): %s\n", key, value, seq)
	}

	for ; ; seq = seq.next() {
		g := m.ctrls.At(seq.offset)
		match := g.matchEmptyOrDeleted()
		if debug {
			fmt.Printf("put(probing): offset=%d match-empty=%s [% 02x]\n",
				seq.offset, match, m.ctrls.Slice(seq.offset, seq.offset+groupSize))
		}

		if match != 0 {
			i := seq.offsetAt(match.next())
			slot := m.slots.At(i)
			slot.key = key
			slot.value = value
			m.setCtrl(i, ctrl(h2(h)))
			m.growthLeft--
			if debug {
				fmt.Printf("put(inserting): index=%d used=%d growth-left=%d\n", i, m.used+1, m.growthLeft)
			}
			return
		}
	}
}

// rehash resize the capacity of the table by allocating a bigger array and
// uncheckedPutting each element of the table into the new array (we know that
// no insertion here will Put an already-present value), and discard the old
// backing array.
func (m *M[K, V]) rehash(newCapacity uintptr) {
	// TODO(peter): rehash in place if there are a sufficient number of
	// tombstones to reclaim. See drop_deletes_without_resize() in the abseil
	// implementation:
	// https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h#L311

	if (1 + newCapacity) < groupSize {
		newCapacity = groupSize - 1
	}

	oldCtrls, oldSlots := m.ctrls, m.slots
	m.slots = makeUnsafeSlice(make([]slot[K, V], newCapacity))
	m.ctrls = makeUnsafeSlice(make([]ctrl, newCapacity+groupSize))
	for i := uintptr(0); i < newCapacity+groupSize; i++ {
		*m.ctrls.At(i) = ctrlEmpty
	}
	*m.ctrls.At(newCapacity) = ctrlSentinel

	if newCapacity < groupSize {
		// If the map fits in a single group then we're able to fill all of
		// the slots except 1 (an empty slot is needed to terminate find
		// operations).
		m.growthLeft = int(newCapacity - 1)
	} else {
		m.growthLeft = int((newCapacity * maxAvgGroupLoad) / groupSize)
	}

	oldCapacity := m.capacity
	m.capacity = newCapacity

	if debug {
		fmt.Printf("rehash: capacity=%d->%d  growth-left=%d\n",
			oldCapacity, newCapacity, m.growthLeft)
	}

	for i := uintptr(0); i < oldCapacity; i++ {
		c := *oldCtrls.At(i)
		if c == ctrlEmpty || c == ctrlDeleted {
			continue
		}
		slot := oldSlots.At(i)
		h := m.hash(noescape(unsafe.Pointer(&slot.key)), m.seed)
		m.uncheckedPut(h, slot.key, slot.value)
	}
}

type bitset uint64

func (b bitset) next() uintptr {
	return uintptr(bits.TrailingZeros64(uint64(b))) >> 3
}

func (b bitset) clear(i uintptr) bitset {
	return b &^ (bitset(0x80) << (i << 3))
}

func (b bitset) count() int {
	return bits.OnesCount64(uint64(b))
}

func (b bitset) String() string {
	var buf strings.Builder
	buf.Grow(groupSize)
	for i := 0; i < groupSize; i++ {
		if (b & (bitset(0x80) << (i << 3))) != 0 {
			buf.WriteString("1")
		} else {
			buf.WriteString("0")
		}
	}
	return buf.String()
}

// Each slot in the hash table has a control byte which can have one of four
// states: empty, deleted, full and the sentinel. They have the following bit
// patterns:
//
//	   empty: 1 0 0 0 0 0 0 0
//	 deleted: 1 1 1 1 1 1 1 0
//	    full: 0 h h h h h h h  // h represents the H1 hash bits
//	sentinel: 1 1 1 1 1 1 1 1
type ctrl uint8

var emptyCtrls = func() unsafeSlice[ctrl] {
	v := make([]ctrl, groupSize)
	for i := range v {
		v[i] = ctrlEmpty
	}
	return makeUnsafeSlice(v)
}()

func (c *ctrl) matchH2(h uintptr) bitset {
	v := *(*uint64)((unsafe.Pointer)(c)) ^ (bitsetLSB * uint64(h))
	return bitset(((v - bitsetLSB) &^ v) & bitsetMSB)
}

func (c *ctrl) matchEmpty() bitset {
	v := *(*uint64)((unsafe.Pointer)(c))
	return bitset((v &^ (v << 6)) & bitsetMSB)
}

func (c *ctrl) matchFull() bitset {
	v := *(*uint64)((unsafe.Pointer)(c))
	return bitset((v ^ bitsetMSB) & bitsetMSB)
}

func (c *ctrl) matchEmptyOrDeleted() bitset {
	v := *(*uint64)((unsafe.Pointer)(c))
	return bitset((v &^ (v << 7)) & bitsetMSB)
}

// probeSeq maintains the state for a probe sequence. The sequence is a
// triangular progression of the form
//
//	p(i) := groupSize * (i^2 + i)/2 + hash (mod mask+1)
//
// The use of groupSize ensures that each probe step does not overlap groups;
// the sequence effectively outputs the addresses of *groups* (although not
// necessarily aligned to any boundary). The group machinery allows us to
// check an entire group with minimal branching.
//
// Wrapping around at mask+1 is important, but not for the obvious reason. As
// described above, the first few entries of the control byte array are
// mirrored at the end of the array, which group will find and use for
// selecting candidates. However, when those candidates' slots are actually
// inspected, there are no corresponding slots for the cloned bytes, so we
// need to make sure we've treated those offsets as "wrapping around".
//
// It turns out that this probe sequence visits every group exactly once if
// the number of groups is a power of two, since (i^2+i)/2 is a bijection in
// Z/(2^m). See https://en.wikipedia.org/wiki/Quadratic_probing
type probeSeq struct {
	mask   uintptr
	offset uintptr
	index  uintptr
}

func makeProbeSeq(hash uintptr, mask uintptr) probeSeq {
	return probeSeq{
		mask:   mask,
		offset: hash & mask,
		index:  0,
	}
}

func (s probeSeq) next() probeSeq {
	s.index += groupSize
	s.offset = (s.offset + s.index) & s.mask
	return s
}

func (s probeSeq) offsetAt(i uintptr) uintptr {
	return (s.offset + i) & s.mask
}

func (s probeSeq) String() string {
	return fmt.Sprintf("mask=%d offset=%d index=%d", s.mask, s.offset, s.index)
}

// Extracts the H1 portion of a hash: the 57 upper bits.
func h1(h uintptr) uintptr {
	return h >> 7
}

// Extracts the H2 portion of a hash: the 7 bits not used for h1.
//
// These are used as an occupied control byte.
func h2(h uintptr) uintptr {
	return h & 0x7f
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

// unsafeSlice provides semi-ergonomic limited slice-like functionality
// without bounds checking for fixed sized slices.
type unsafeSlice[T any] struct {
	ptr unsafe.Pointer
}

func makeUnsafeSlice[T any](s []T) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(unsafe.SliceData(s))}
}

// At returns a pointer to the element at index i.
func (s unsafeSlice[T]) At(i uintptr) *T {
	var t T
	return (*T)(unsafe.Add(s.ptr, unsafe.Sizeof(t)*i))
}

// Slice returns a Go slice akin to slice[start:end] for a Go builtin slice.
func (s unsafeSlice[T]) Slice(start, end uintptr) []T {
	return unsafe.Slice((*T)(s.ptr), end)[start:end]
}
