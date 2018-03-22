// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rate

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Counter can be used to compute a live rate at which a count of events
// is occurring.  The zero value of a Counter is ready to use.
//
// Internally, it uses an EWMA that will automatically
// decay over one minute.  The value of the counter can be permanently
// frozen to preserve its value once no new events are expected.
type Counter struct {
	mu struct {
		syncutil.Mutex
		// frozen saves the last-known rate.Value() since it will decay
		// over time.
		frozen Rate
		// Use EWMA, and allow it to be discarded once frozen.
		rate  *metric.Rate
		ready bool
	}
}

var _ Rater = &Counter{}

// Add a number of events to the counter.  This method will be a no-op
// if the Counter has been frozen.
func (c *Counter) Add(count int) {
	c.mu.Lock()
	switch {
	case !c.mu.ready:
		c.mu.ready = true
		c.mu.rate = metric.NewRate(time.Minute)
		fallthrough
	case c.mu.rate != nil:
		c.mu.rate.Add(float64(count))
	}
	c.mu.Unlock()
}

// Freeze the rate in the counter.  Once this method is called,
// it is still safe to call Add(), however no updates will occur.
func (c *Counter) Freeze() {
	c.mu.Lock()
	if c.mu.rate != nil {
		c.mu.frozen = NewRate(c.mu.rate.Value(), time.Second)
		c.mu.rate = nil
		c.mu.ready = true
	}
	c.mu.Unlock()
}

// IsFrozen indicates if freeze() has been called.
func (c *Counter) IsFrozen() (ret bool) {
	c.mu.Lock()
	ret = c.mu.ready && c.mu.rate == nil
	c.mu.Unlock()
	return
}

// Rate returns the live or frozen rate of events.
func (c *Counter) Rate() (ret Rate) {
	c.mu.Lock()
	if c.mu.rate != nil {
		ret = NewRate(c.mu.rate.Value(), time.Second)
	} else {
		ret = c.mu.frozen
	}
	c.mu.Unlock()
	return
}

// Reset returns the Counter to its zero-value.
func (c *Counter) Reset() {
	c.mu.Lock()
	c.mu.frozen = 0
	c.mu.rate = metric.NewRate(time.Minute)
	c.mu.ready = true
	c.mu.Unlock()
}

// String shows counts / sec.
func (c *Counter) String() string {
	return c.Rate().String()
}

// NewChannelRater constructs a Rater that will receive counts
// from the given channel.  The rate will be frozen once
// the channel is closed.
func NewChannelRater(ch <-chan int) Rater {
	ctr := &Counter{}

	go func() {
		for count := range ch {
			ctr.Add(count)
		}
		ctr.Freeze()
	}()

	return ctr
}
