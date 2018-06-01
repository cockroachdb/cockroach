package tombstonemap

import (
	"bytes"
	"fmt"
	"sort"
)

type entry struct {
	begin string
	end   string
	seq   int
}

func (e entry) String() string {
	return fmt.Sprintf("%s-%s#%d", e.begin, e.end, e.seq)
}

type entries []entry

func (e entries) Len() int {
	return len(e)
}

func (e entries) Less(i, j int) bool {
	if e[i].begin < e[j].begin {
		return true
	}
	if e[i].begin > e[j].begin {
		return false
	}
	return e[i].seq > e[j].seq
}

func (e entries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// M maintains a map of versioned tombstones and provides O(logn + k)
// operations for adding a tombstone and determining whether a key is deleted
// at a particular version (sequence number).
//
// The natural representation for this tombstone map would be an interval tree,
// yet we need to work within the confines of the RocksDB iterator API. The
// difficulty with this restriction is avoiding having to examine all of the
// entries in the map. Consider the following scenario:
//
//   3: a-----------m
//   2:      f------------s
//   1:          j---------------z
//
// Imagine we index the tombstones by their start key. What happens if we query
// for the key "t". We search in the tree and find the tombstone j-z#1. Great,
// but how do we know this is the only tombstone that covers this key? We
// don't. We have to look at the previous tombstone and the one before that,
// etc, all the way to the tombstone with the smallest begin key because we
// don't know how far each of these tombstones extend.
//
// In order to avoid this O(n) scanning operation, this maintains the invariant
// that any tombstone start key will be the start key for the start key or end
// key for any other overlapping tombstone. Tombstones are split upon being
// added to the map and we split existing tombstones in the map based on the
// new tombstone's begin key. In the example above, we create a structure that
// looks like:
//
//   3: a----f---j--m
//   2:      f---j--------s
//   1:          j---------------z
//
// This expands the number of tombstones, but allows query operations to know
// at which point there are no interesting tombstones to the left of a key.
//
// Note that overlapping tombstones do create pathological behavior here
// (quadratic number of tombstones can be generated), yet overlapping
// tombstones are rare in practice. If tombstones do not overlap, not
// additional tombstones are created. The above structure can be encoded and
// queried directly in the RocksDB memtable and sstables.
//
// The approach here is reminiscent of the CockroachDB timestamp cache, though
// the timestamp cache is mildly simpler in that it only needs to maintain a
// single value (the max timestamp) for overlapping ranges.
type M struct {
	// Tombstone map entries are contained in a single sorted slice. A real
	// implementation would use a tree.
	entries entries
}

// New constructs a new tombstone map.
func New() *M {
	return &M{}
}

func (m *M) String() string {
	var buf bytes.Buffer
	for _, e := range m.entries {
		fmt.Fprintf(&buf, "%s\n", e)
	}
	return buf.String()
}

// index returns the index of the first entry in the map the potentially
// overlaps the specified key. It is up to the caller to determine if an
// overlap actually exists. This method is O(logn + k) where n is the total
// number of tombstones and k is the number of overlapping tombstones at the
// specified key.
func (m *M) index(key string) int {
	if len(m.entries) == 0 {
		return -1
	}

	i := sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].begin >= key
	})

	// i is currently positioned at the first entry which is greater than or
	// equal to key.
	if i > 0 {
		// Entries are sorted by key and then by descending sequence
		// number. Consider what happens if we have multiple entries beginning on a
		// particular key, such as [a-b#3, a-c#2, a-d#1]. If we're searching for
		// the key "b", we'll be positioned after that last entry. We need to back
		// up until we reach the first entry.
		if i >= len(m.entries) || m.entries[i].begin != key {
			i--
			key = m.entries[i].begin
		}
		for ; i > 0; i-- {
			e := &m.entries[i-1]
			if e.begin != key {
				break
			}
		}
	}

	return i
}

// Add adds a tombstone from begin (inclusive) to end (exclusive) at the
// specified sequence number.
func (m *M) Add(begin, end string, seq int) {
	i := m.index(begin)
	n := len(m.entries)
	if i >= 0 {
		// Split existing entries at begin.
		for ; i < n; i++ {
			e := &m.entries[i]
			if begin <= e.begin {
				break
			}
			if begin < e.end {
				m.entries = append(m.entries, entry{begin, e.end, e.seq})
				m.entries[i].end = begin
			}
		}

		// Split the current entry based on the overlapping begin points of
		// existing entries.
		for ; i < n; i++ {
			e := &m.entries[i]
			if begin >= e.end {
				break
			}
			if begin < e.begin && end > e.begin {
				m.entries = append(m.entries, entry{begin, e.begin, seq})
				begin = e.begin
			}
		}
	}

	if begin != end {
		m.entries = append(m.entries, entry{begin, end, seq})
	}
	sort.Sort(m.entries)
}

// Get returns true if the specified <key,seq> pair is deleted at the specified
// read sequence number. Get ignores tombstones newer than the read sequence
// number. This method is O(logn + k) where n is the total number of tombstones
// and k is the number of overlapping tombstones at the specified key.
func (m *M) Get(key string, seq, readSeq int) bool {
	i := m.index(key)
	if i < 0 {
		return false
	}
	for ; i < len(m.entries); i++ {
		e := &m.entries[i]
		if key < e.begin {
			break
		}
		if e.seq > readSeq {
			// Ignore tombstones newer than our read sequence.
			continue
		}
		if e.seq >= seq && key >= e.begin && key < e.end {
			// The key lies within the tombstone and the tombstone is newer than the
			// key.
			return true
		}
	}
	return false
}
