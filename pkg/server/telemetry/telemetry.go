// Copyright 2019 The Cockroach Authors.
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

package telemetry

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

const (
	baseMarkerPath    = "${HOME}"
	baseMarkerPathEnv = "COCKROACH_INSTALL_UUID_PATH"
	noTelemetryEnv    = "COCKROACH_NO_TELEMETRY"
)

var (
	// Data is cached so we don't have to keep looking on-disk for the
	// install UUID.
	cachedSources struct {
		syncutil.Mutex
		data map[string]TelemetrySource
	}
	noTelemetry bool
	markerPath  string
	startTime   = timeutil.Now()
)

func init() {
	cachedSources.data = make(map[string]TelemetrySource)
	noTelemetry = envutil.EnvOrDefaultString(noTelemetryEnv, "") != ""
	markerPath = envutil.EnvOrDefaultString(baseMarkerPathEnv, baseMarkerPath)
}

// installUUID returns a UUID which has been stored in a local file.
func installUUID(name string) (uuid.UUID, error) {
	uuidPath := os.ExpandEnv(filepath.Join(os.ExpandEnv(markerPath), "."+name, "install-uuid"))
	data, err := ioutil.ReadFile(uuidPath)
	if err != nil {
		if os.IsNotExist(err) {
			ret := uuid.FastMakeV4()
			if err := os.MkdirAll(filepath.Dir(uuidPath), 0755); err != nil {
				return uuid.Nil, errors.Wrap(err, "unable to make installation uuid store")
			}
			if err := ioutil.WriteFile(uuidPath, []byte(ret.String()), 0644); err != nil {
				return uuid.Nil, err
			}
			return ret, nil
		}
		return uuid.Nil, err
	}
	return uuid.FromString(string(data))
}

// NewTelemetrySource returns a TelemetrySource which has the Product,
// and InstallUUID fields pre-populated.
//
// TODO(bob): Once we upgrade to go 1.12 and modularize, automatically
// populate the Version field with the main module info.
func NewTelemetrySource(product string) (*TelemetrySource, error) {
	if product == "" {
		return nil, errors.New("product must not be empty")
	}

	cachedSources.Lock()
	ret, found := cachedSources.data[product]
	if !found {
		id, err := installUUID(product)
		if err != nil {
			return nil, err
		}

		ret.Product = product
		ret.InstallUUID = id.String()
		// ret.Version = runtime.debug.ReadBuildInfo().Main.Version
		cachedSources.data[product] = ret
	}
	cachedSources.Unlock()
	return &ret, nil
}

// SetUptime updates the Uptime field in the given source and returns it.
//
// This is called automatically by TelemetryReport.Send().
func (m *TelemetrySource) SetUptime() time.Duration {
	m.Uptime = timeutil.Now().Sub(startTime)
	return m.Uptime
}

// NewTelemetryReport constructs a report from a source and feature collection.
func NewTelemetryReport(source *TelemetrySource, features map[string]int32) *TelemetryReport {
	return &TelemetryReport{
		Source:       *source,
		FeatureUsage: features,
	}
}

// Send will transmit report via an HTTP POST to the given URL.
//
// If the COCKROACH_NO_TELEMETRY environment variable is present, this
// method is a no-op.
func (m *TelemetryReport) Send(ctx context.Context, url string) error {
	if noTelemetry {
		return nil
	}

	m.Source.SetUptime()

	b, err := protoutil.Marshal(m)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-protobuf")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.Wrap(err, "unable to read response body")
		}
		return errors.Errorf("failed to send telemetry: status %s, body: %s", res.Status, string(body))
	}

	return nil
}
