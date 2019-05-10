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
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const baseTelemetryProduct = "roachprod"
const baseTelemetryURL = "https://telemetry-staging.crdb.dev/api/telemetry/report"

var (
	telemetryProduct string
	telemetryURL     *url.URL
)

func init() {
	var err error
	telemetryProduct = envutil.EnvOrDefaultString("COCKROACH_TELEMETRY_PRODUCT", baseTelemetryProduct)
	telemetryURL, err = url.Parse(envutil.EnvOrDefaultString("COCKROACH_TELEMETRY_URL", baseTelemetryURL))
	if err != nil {
		panic(err)
	}
}

func event(err error, attrs ...string) {
	suffix := strings.Join(attrs[1:], ".")
	if err == nil {
		telemetry.Count(fmt.Sprintf("%s.ok.%s", attrs[0], suffix))
	} else {
		telemetry.Count(fmt.Sprintf("%s.error.%s", attrs[0], suffix))
	}
}

// CreateVM records the creation of a VM
func CreateVM(name, provider, zone, machineType string, err error) {
	event(err, "create", name, provider, zone, machineType)
}

// DestroyVM records the destruction of a VM.
// The gc parameter indicates whether or not this was the result of a
// garbage-collection event or a user-initiated command.
func DestroyVM(vm vm.VM, gc bool, err error) {
	kind := "destroy"
	if gc {
		kind = "gc"
	}
	event(err, kind, vm.Name, vm.Provider, vm.Zone, vm.MachineType)
	if err == nil {
		seconds := int(timeutil.Now().Sub(vm.CreatedAt).Seconds())
		event(nil, kind, "lifetime", vm.Name, strconv.Itoa(seconds))
	}
}

// Send uploads telemetry counters.
func Send(ctx context.Context) error {
	source, err := telemetry.NewTelemetrySource(telemetryProduct)
	if err != nil {
		return err
	}

	rep := telemetry.NewTelemetryReport(source, telemetry.GetFeatureCounts(false, true))

	return rep.Send(ctx, telemetryURL.String())
}
