package gopter

import "sync/atomic"

// Flag is a convenient helper for an atomic boolean
type Flag struct {
	flag int32
}

// Get the value of the flag
func (f *Flag) Get() bool {
	return atomic.LoadInt32(&f.flag) > 0
}

// Set the the flag
func (f *Flag) Set() {
	atomic.StoreInt32(&f.flag, 1)
}

// Unset the flag
func (f *Flag) Unset() {
	atomic.StoreInt32(&f.flag, 0)
}
