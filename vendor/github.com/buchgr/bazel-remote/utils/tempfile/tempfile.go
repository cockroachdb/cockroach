package tempfile

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// Creator maintains the state of a pseudo-random number generator
// used to create temp files.
type Creator struct {
	mu   sync.Mutex
	idum uint32 // Pseudo-random number generator state.
}

// NewCreator returns a new Creator, for creating temp files.
func NewCreator() *Creator {
	return &Creator{idum: uint32(time.Now().UnixNano())}
}

// Fast "quick and dirty" linear congruential (pseudo-random) number
// generator from Numerical Recipes. Excerpt here:
// https://www.unf.edu/~cwinton/html/cop4300/s09/class.notes/LCGinfo.pdf
// This is the same algorithm as used in the ioutil.TempFile go standard
// library function.
func (c *Creator) ranqd1() string {
	c.mu.Lock()
	c.idum = c.idum*1664525 + 1013904223
	r := c.idum
	c.mu.Unlock()
	return strconv.Itoa(int(1e9 + r%1e9))[1:]
}

const flags = os.O_RDWR | os.O_CREATE | os.O_EXCL
const mode = 0666 // Before the umask is applied.

// Create attempts to create a temp file for eventualFile, and returns
// the corresponding *os.File and an error if any occurred. The temp
// file is created in the same directory as eventualFile, with permissions
// 0666 before the umask is applied.
func (c *Creator) Create(eventualFile string) (*os.File, error) {
	var err error
	var f *os.File

	base := eventualFile + ".tmp"

	for i := 0; i < 10000; i++ {
		name := base + c.ranqd1()
		f, err = os.OpenFile(name, flags, mode)
		if os.IsExist(err) {
			continue
		}
		return f, err
	}
	return nil, err
}
