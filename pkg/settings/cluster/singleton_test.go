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

package cluster

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCrashReportingSingletonSetting(t *testing.T) {
	st := MakeClusterSettings()

	for _, hasDiagnosticsReportingEnabled := range []bool{false, true} {
		for _, hasCrashReportsEnabled := range []bool{false, true} {
			st.ReportingSettings.DiagnosticsReportingEnabled.Override(hasDiagnosticsReportingEnabled)
			st.ReportingSettings.CrashReports.Override(hasCrashReportsEnabled)
			s := *log.ReportingSettingsSingleton.Load().(*log.ReportingSettings)

			act, exp :=
				s.HasDiagnosticsReportingEnabled(), st.ReportingSettings.HasDiagnosticsReportingEnabled()
			if act != exp {
				t.Fatalf("(%t, %t): actual %t != expected %t",
					hasDiagnosticsReportingEnabled, hasCrashReportsEnabled, act, exp)
			}
		}
	}
}
