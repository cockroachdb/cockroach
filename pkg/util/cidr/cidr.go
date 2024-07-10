// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cidr

import (
	"context"
	"encoding/json"
	io "io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var cidrMappingUrl = settings.RegisterStringSetting(
	settings.SystemVisible,
	"server.cidr_mapping_url",
	"url of a JSON file containing a list of CIDR blocks (file:// or http://)",
	envutil.EnvOrDefaultString("COCKROACH_CIDR_MAPPING", ""),
)

var cidrRefreshInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"server.cidr_refresh_interval",
	"interval at which to refresh the CIDR mapping, 0 means don't refresh",
	0,
)

// Lookup looks up the CIDR record for either an IP address or a URL. The source
// for the mapping is controlled by the cluster setting
// server.cidr_mapping_url. The mapping is periodically refreshed.
type Lookup struct {
	// byLength is an array by length of a map of an IP prefix to a destination
	// name. This is repopulated whenever SetURL is changed.
	byLength atomic.Pointer[[]map[string]string]

	st *settings.Values

	// lastUpdate is the last time the contents in the CIDR URL were changed.
	lastUpdate atomic.Value

	// changed is used to signal that the configuration has changed and forces a reload.
	changed chan time.Duration

	// onChange is a list of functions to call when the CIDR mapping is updated.
	changeMu struct {
		syncutil.Mutex
		onChange []func(ctx context.Context)
	}
}

// NewLookup creates a new Lookup. It will not return any results until Start is called.
func NewLookup(st *settings.Values) *Lookup {
	c := &Lookup{st: st}
	byLength := make([]map[string]string, 0)
	c.byLength.Store(&byLength)
	c.lastUpdate.Store(time.Time{})
	c.changed = make(chan time.Duration)

	cidrMappingUrl.SetOnChange(st, func(ctx context.Context) {
		// Reset the lastUpdate time so that the URL is always reloaded even if
		// the new file/URL has an older timestamp.
		c.lastUpdate.Store(time.Time{})
		c.changed <- cidrRefreshInterval.Get(c.st)
	})
	// We have to register this callback first. Otherwise we may run into
	// an unlikely but possible scenario where we've started the ticker,
	// and the setting is changed before we register the callback and the
	// ticker will not be reset to the new value.
	cidrRefreshInterval.SetOnChange(c.st, func(ctx context.Context) {
		c.changed <- cidrRefreshInterval.Get(c.st)
	})
	return c
}

// Start refreshes the lookup once and begins the CIDR lookup refresh task.
func (c *Lookup) Start(ctx context.Context, stopper *stop.Stopper) (bool, *Lookup) {
	getTickDuration := func() time.Duration {
		tickDuration := cidrRefreshInterval.Get(c.st)
		// If the tickDuration is 0, set to a year to avoid auto refreshing.
		if tickDuration == 0 {
			tickDuration = time.Hour * 24 * 365
		}
		return tickDuration
	}
	if err := stopper.RunAsyncTask(ctx, "cidr-refresh", func(ctx context.Context) {
		// Refresh once before starting the ticker.
		c.refresh(ctx)
		ticker := time.NewTicker(getTickDuration())
		defer ticker.Stop()
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				c.refresh(ctx)
			case <-c.changed:
				c.refresh(ctx)
				ticker.Reset(getTickDuration())
			}
		}
	}); err != nil {
		log.Fatalf(ctx, "unable to start CIDR lookup refresh task: %v", err)
	}
	return false, nil
}

// hexString returns a hex string representation of an IP address. The length of
// the string will always be twice the length of the input.
func hexString(b []byte) string {
	const hexDigit = "0123456789abcdef"
	s := make([]byte, len(b)*2)
	for i, tn := range b {
		s[i*2], s[i*2+1] = hexDigit[tn>>4], hexDigit[tn&0xf]
	}
	return string(s)
}

// refresh is called to update the CIDR mapping. It checks if the URL has been
// recently updated and if so, it will reload the mapping.
func (c *Lookup) refresh(ctx context.Context) {
	// Check if the URL is updated
	if c.isUpdated(ctx, cidrMappingUrl.Get(c.st)) {
		// Set the URL
		url := cidrMappingUrl.Get(c.st)
		if err := c.setURL(ctx, url); err != nil {
			log.Errorf(ctx, "error setting CIDR URL to '%s': %v", url, err)
		}
	}
}

// isUpdated checks if the URL has been updated since the last time it was
// loaded.
func (c *Lookup) isUpdated(ctx context.Context, rawURL string) bool {
	// Check if the URL is a local file
	if strings.HasPrefix(rawURL, "file://") {
		// Extract the file path from the URL
		filePath := strings.TrimPrefix(rawURL, "file://")

		// Get the file information
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return false
		}

		// Compare the modification time of the file with lastUpdate
		if fileInfo.ModTime().After(c.lastUpdate.Load().(time.Time)) {
			c.lastUpdate.Store(fileInfo.ModTime())
			return true
		}
	} else if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		// Create an HTTP client
		client := &http.Client{}

		// Send a HEAD request to the URL
		resp, err := client.Head(rawURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		// Get the Last-Modified header from the response
		lastModified := resp.Header.Get("Last-Modified")
		if lastModified == "" {
			return false
		}

		// Parse the Last-Modified header
		modTime, err := http.ParseTime(lastModified)
		if err != nil {
			return false
		}

		// Compare the modification time with lastUpdate
		if modTime.After(c.lastUpdate.Load().(time.Time)) {
			c.lastUpdate.Store(modTime)
		}
	} else if rawURL == "" {
		if c.lastUpdate.Load().(time.Time).IsZero() {
			c.lastUpdate.Store(timeutil.Now())
			return true
		}
	}

	return false
}

// setURL sets the URL for the CIDR lookup. The URL can be a local file or an
// HTTP URL. Once the file is loaded it is optionally transformed and then
// converted into CIDR records and loaded.
func (c *Lookup) setURL(ctx context.Context, rawURL string) error {
	contents := []byte("[]")
	var err error
	// Check if the URL is a local file
	if strings.HasPrefix(rawURL, "file://") {
		// Extract the file path from the URL
		filePath := strings.TrimPrefix(rawURL, "file://")

		// Read the file contents
		if contents, err = os.ReadFile(filePath); err != nil {
			return err
		}
	} else if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		// Create an HTTP client
		client := &http.Client{}

		// Send a GET request to the URL
		resp, err := client.Get(rawURL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Read the response body
		if contents, err = io.ReadAll(resp.Body); err != nil {
			return err
		}
	}

	if err := c.setDestinations(ctx, contents); err != nil {
		return err
	}
	return nil
}

// cidr represents a single cidr entry.
type cidr struct {
	// Human readable name the metric will be published under.
	Name string
	// CIDR block that can be processed by net.ParseCIDR
	Ipnet string
}

// setDestinations sets the destinations for the CIDR lookup. Note that it
// atomically updates byLength at the end rather than modifying it in place.
func (c *Lookup) setDestinations(ctx context.Context, contents []byte) error {
	var destinations []cidr
	if err := json.Unmarshal(contents, &destinations); err != nil {
		return err
	}
	// TODO(baptist): This only handles IPv4. We could change to 128 if we want
	// to handle IPv6.
	byLength := make([]map[string]string, 33)
	for i := range 33 {
		byLength[i] = make(map[string]string)
	}
	for _, d := range destinations {
		_, cidr, err := net.ParseCIDR(d.Ipnet)
		if err != nil {
			return err
		}
		lenBits, _ := cidr.Mask.Size()
		mask := net.CIDRMask(lenBits, 32)
		val := hexString(cidr.IP.Mask(mask))
		byLength[lenBits][val] = d.Name
	}
	log.Infof(ctx, "CIDR lookup updated with %d destinations", len(destinations))
	c.byLength.Store(&byLength)
	c.onChange(ctx)
	return nil
}

// SetOnChange installs a callback to be called when the CIDR mapping is updated.
func (c *Lookup) SetOnChange(fn func(ctx context.Context)) {
	c.changeMu.Lock()
	defer c.changeMu.Unlock()
	c.changeMu.onChange = append(c.changeMu.onChange, fn)
}

// onChange calls all the registered callbacks.
func (c *Lookup) onChange(ctx context.Context) {
	// Drop the changeMu lock before calling the callbacks.
	var onChange []func(ctx context.Context)
	func() {
		c.changeMu.Lock()
		defer c.changeMu.Unlock()
		onChange = make([]func(ctx context.Context), len(c.changeMu.onChange))
		copy(onChange, c.changeMu.onChange)
	}()

	for _, fn := range onChange {
		fn(ctx)
	}
}

// LookupIP looks up the name for the best matching cidr for an IP by going
// through all possible lengths from shortest to longest and checking if the
// prefix matches.
func (c *Lookup) LookupIP(ip net.IP) string {
	byLength := *c.byLength.Load()
	ip = ip.To4()
	for i := len(byLength) - 1; i >= 0; i-- {
		m := (byLength)[i]
		if len(m) == 0 {
			continue
		}
		mask := net.CIDRMask(i, 32)
		val := hexString(ip.Mask(mask))
		if m[val] != "" {
			return m[val]
		}
	}
	return ""
}
