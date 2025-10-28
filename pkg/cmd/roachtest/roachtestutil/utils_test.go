// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdLogFileName(t *testing.T) {
	ts := time.Date(2000, 1, 1, 15, 4, 12, 0, time.Local)

	const exp = `run_150412.000000000_n1,3-4,9_cockroach-bla-foo-ba`
	nodes := option.NodeListOption{1, 3, 4, 9}
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach", "bla", "--foo", "bar"),
	)
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach bla --foo bar"),
	)
}

func TestPrintProvidersErrors(t *testing.T) {
	testCases := []struct {
		name           string
		providersState map[string]string
		expectedLines  []string
	}{
		{
			name: "No errors",
			providersState: map[string]string{
				aws.ProviderName:   "Active",
				gce.ProviderName:   "Active",
				azure.ProviderName: "Active",
				ibm.ProviderName:   "Active",
				local.ProviderName: "Active",
			},
		},
		{
			name: "Single error",
			providersState: map[string]string{
				aws.ProviderName:   "Active",
				gce.ProviderName:   "Inactive - failed to authenticate",
				azure.ProviderName: "Active",
				ibm.ProviderName:   "Active",
				local.ProviderName: "Active",
			},
			expectedLines: []string{
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", gce.ProviderName),
			},
		},
		{
			name: "Multiple errors",
			providersState: map[string]string{
				aws.ProviderName:   "Active",
				gce.ProviderName:   "Inactive - failed to authenticate",
				azure.ProviderName: "Inactive - failed to authenticate",
				ibm.ProviderName:   "Active",
				local.ProviderName: "Active",
			},
			expectedLines: []string{
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", gce.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", azure.ProviderName),
			},
		},
		{
			name: "No errors with disabled provider",
			providersState: map[string]string{
				aws.ProviderName:   "Active",
				gce.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				azure.ProviderName: "Active",
				ibm.ProviderName:   "Active",
				local.ProviderName: "Active",
			},
		},
		{
			name: "No errors with multiple disabled providers",
			providersState: map[string]string{
				aws.ProviderName:   "Active",
				gce.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				azure.ProviderName: fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				ibm.ProviderName:   "Active",
				local.ProviderName: "Active",
			},
		},
		{
			name: "Errors with multiple disabled providers",
			providersState: map[string]string{
				aws.ProviderName:   "Inactive - failed to authenticate",
				gce.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				azure.ProviderName: fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				ibm.ProviderName:   "Inactive - failed to authenticate",
				local.ProviderName: "Active",
			},
			expectedLines: []string{
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", aws.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", ibm.ProviderName),
			},
		},
		{
			name: "All Errors",
			providersState: map[string]string{
				aws.ProviderName:   "Inactive - failed to authenticate",
				gce.ProviderName:   "Inactive - failed to authenticate",
				azure.ProviderName: "Inactive - failed to authenticate",
				ibm.ProviderName:   "Inactive - failed to authenticate",
				local.ProviderName: "Inactive - failed to authenticate",
			},
			expectedLines: []string{
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", aws.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", gce.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", azure.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", ibm.ProviderName),
				fmt.Sprintf("Error from cloud provider %s: Inactive - failed to authenticate", local.ProviderName),
			},
		},
		{
			name: "All Disabled",
			providersState: map[string]string{
				aws.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				gce.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				azure.ProviderName: fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				ibm.ProviderName:   fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
				local.ProviderName: fmt.Sprintf("Inactive - %s", roachprod.RoachprodDisabledProviderMessage),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			PrintProvidersErrors(&buf, tc.providersState)
			var outputLines []string
			if buf.String() != "" {
				// if buffer is empty, then buf.String() will return an empty string
				// passing an empty string into strings.Split will return a slice with
				// a single element
				// for length assertion later, if buffer is empty, outputLines should
				// be nil

				outputLines = strings.Split(strings.TrimSpace(buf.String()), "\n")
			}
			require.Len(t, outputLines, len(tc.expectedLines))
			for _, line := range outputLines {
				require.True(t, slices.Contains(tc.expectedLines, line))
			}
		})
	}
}
