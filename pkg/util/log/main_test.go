// Copyright 2017 The Cockroach Authors.
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

package log_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type dummyReportingSettings struct {
	diagnosticsReportingEnabled, crashReportsEnabled bool
}

func (d dummyReportingSettings) HasDiagnosticsReportingEnabled() bool {
	return d.diagnosticsReportingEnabled
}
func (d dummyReportingSettings) HasCrashReportsEnabled() bool {
	return d.crashReportsEnabled
}

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	f := log.ReportingSettings(dummyReportingSettings{})
	log.ReportingSettingsSingleton.Store(&f)
	os.Exit(m.Run())
}
