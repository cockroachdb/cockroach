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
	cjson "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var cidrMappingUrl = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"metrics.cidr_mapping_url",
	"url of a JSON file containing a list of CIDR blocks (file:// or http://)",
	envutil.EnvOrDefaultString("COCKROACH_CIDR_MAPPING", ""),
	settings.WithPublic,
)

var cidrMappingTransform = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"metrics.cidr_mapping_transform",
	"optional transform to apply to the JSON content",
	"",
	settings.WithPublic,
)

type IpLookup func(ip net.IP) string

// CIDR is a way to name a CIDR block.
type CIDR struct {
	// Human readable name the metric will be published under.
	Name string
	// CIDR block that can be processed by net.ParseCIDR
	Ipnet string
}

type CIDRLookup struct {
	// byLength is an array by length of a map of an IP prefix to a destination
	// name. This is repopulated whenever SetURL is changed.
	byLength atomic.Pointer[[]map[string]string]

	st *settings.Values

	// lastUpdate is the last time the contents pointed to by the CIDR URL were updated.
	lastUpdate time.Time

	// Only used by tests - remove
	transform string
}

// update every minute if the contents change
const refreshInterval = 10 * time.Second

func NewCIDRLookup(ctx context.Context, st *settings.Values, stopper *stop.Stopper) *CIDRLookup {
	c := &CIDRLookup{st: st}
	c.refresh(ctx)

	cidrMappingUrl.SetOnChange(st, func(ctx context.Context) {
		c.lastUpdate = time.Time{}
		c.refresh(ctx)
	})

	if err := stopper.RunAsyncTask(ctx, "cidr-refresh", func(ctx context.Context) {
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				c.refresh(ctx)
			}
		}
	}); err != nil {
		log.Fatalf(ctx, "unable to start CIDR lookup refresh task: %v", err)
	}
	return c
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

func (c *CIDRLookup) refresh(ctx context.Context) {
	// Check if the URL is updated
	if c.isUpdated(ctx, cidrMappingUrl.Get(c.st)) {
		// Set the URL
		url := cidrMappingUrl.Get(c.st)
		if err := c.setURL(ctx, url); err != nil {
			log.Errorf(ctx, "error setting CIDR URL to '%s': %v", url, err)
		}
	}
}

func (c *CIDRLookup) isUpdated(ctx context.Context, rawURL string) bool {
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
		if fileInfo.ModTime().After(c.lastUpdate) {
			c.lastUpdate = fileInfo.ModTime()
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
		if modTime.After(c.lastUpdate) {
			c.lastUpdate = modTime
			return true
		}
	} else if rawURL == "" {
		if c.lastUpdate.IsZero() {
			c.lastUpdate = timeutil.Now()
			return true
		}
	}

	return false
}

// SetURL sets the URL for the CIDR lookup. The URL can be a local file or an
// HTTP URL. Once the file is loaded it is optionally transformed and then
// converted into CIDR records and loaded.
func (c *CIDRLookup) setURL(ctx context.Context, rawURL string) error {
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

	// Apply the transform function to convert to CIDR blocks.
	records, err := c.transformJSON(ctx, contents)
	if err != nil {
		return err
	}
	if err := c.setDestinations(ctx, records); err != nil {
		return err
	}
	return nil
}

// transformJSON is an utility function which converts a file's contents using a
// JQ transform. It takes a JSON doc and applies then JQ transform to get a list
// of [{Name: "name", Ipnet: "ipnet"}, ...] and then converts to []CIDR
func (c *CIDRLookup) transformJSON(
	ctx context.Context, contents []byte,
) (destinations []CIDR, err error) {
	var data any
	if err = json.Unmarshal(contents, &data); err != nil {
		return nil, err
	}
	// Optional transform if set.
	transform := c.transform
	if transform == "" && c.st != nil {
		transform = cidrMappingTransform.Get(c.st)
	}
	if transform != "" {
		data, err = cjson.TransformJson(data, transform)
		if err != nil {
			return nil, err
		}
	}
	var transformedData []map[string]interface{}
	for _, d := range data.([]interface{}) {
		transformedData = append(transformedData, d.(map[string]interface{}))
	}

	destinations = make([]CIDR, len(transformedData))
	for i, j := range transformedData {
		row := j
		destinations[i] = CIDR{Name: row["Name"].(string), Ipnet: row["Ipnet"].(string)}
	}
	return destinations, nil
}

// setDestinations sets the destinations for the CIDR lookup. Note that it
// atomically updates byLength at the end rather than modifying it in place.
func (c *CIDRLookup) setDestinations(ctx context.Context, destinations []CIDR) error {
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
	return nil
}

// Lookup looks up the name for the best matching cidr for an IP by going
// through all possible lengths from shortest to longest and checking if the
// prefix matches.
func (c *CIDRLookup) Lookup(ip net.IP) string {
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
