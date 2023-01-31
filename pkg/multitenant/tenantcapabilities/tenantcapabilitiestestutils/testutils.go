// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiestestutils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

var tenIDRe = regexp.MustCompile(`^{ten=((\d*)|(system))}$`)

// ParseBatchRequestString is a helper function to parse datadriven input that
// declares (empty) batch requests of supported types, for a particular tenant.
// Both the constructed batch request and the requesting tenant ID are returned.
// The input is of the following form:
//
// {ten=10}
// split
// scan
// cput
func ParseBatchRequestString(
	t *testing.T, input string,
) (tenID roachpb.TenantID, ba roachpb.BatchRequest) {
	for i, line := range strings.Split(input, "\n") {
		if i == 0 { // first line describes the tenant ID.
			tenID = ParseTenantID(t, line)
			continue
		}
		switch line {
		case "split":
			ba.Add(&roachpb.AdminSplitRequest{})
		case "scan":
			ba.Add(&roachpb.ScanRequest{})
		case "cput":
			ba.Add(&roachpb.ConditionalPutRequest{})
		default:
			t.Fatalf("unsupported request type: %s", line)
		}
	}
	return tenID, ba
}

// ParseTenantCapabilityUpdateStateArguments is a helper function to parse
// datadriven input to update global tenant capability state. The input is of
// the following form:
//
// upsert {ten=10}:{CanAdminSplit=true}
// delete {ten=20}
func ParseTenantCapabilityUpdateStateArguments(
	t *testing.T, input string,
) (updates []tenantcapabilities.Update) {
	for _, line := range strings.Split(input, "\n") {
		const upsertPrefix, deletePrefix = "upsert ", "delete "
		switch {
		case strings.HasPrefix(line, deletePrefix):
			line = strings.TrimPrefix(line, line[:len(deletePrefix)])
			updates = append(updates, tenantcapabilities.Update{
				Entry: tenantcapabilities.Entry{
					TenantID: ParseTenantID(t, line),
				},
				Deleted: true,
			})
		case strings.HasPrefix(line, upsertPrefix):
			line = strings.TrimPrefix(line, line[:len(upsertPrefix)])
			updates = append(updates, tenantcapabilities.Update{
				Entry:   parseTenantCapabilityEntry(t, line),
				Deleted: false,
			})
		default:
			t.Fatalf("malformed line %q, expected to find prefix %q or %q",
				line, upsertPrefix, deletePrefix)
		}
	}
	return updates
}

// PrintTenantCapabilityUpdate is a helper function that prints out a
// tenantcapabilities.Update, allowing data-driven tests to assert on the
// output.
func PrintTenantCapabilityUpdate(update tenantcapabilities.Update) string {
	if update.Deleted {
		return fmt.Sprintf("deleted %s", printTenantID(update.TenantID))
	}

	return fmt.Sprintf("updated %s", PrintTenantCapabilityEntry(update.Entry))
}

// PrintTenantCapabilityEntry is a helper function that prints out a
// tenantcapabilities.Entry, allowing data-driven tests to assert on the
// output.
func PrintTenantCapabilityEntry(entry tenantcapabilities.Entry) string {
	return fmt.Sprintf(
		"%s:%s",
		printTenantID(entry.TenantID),
		PrintTenantCapability(entry.TenantCapabilities),
	)
}

func parseTenantCapabilityEntry(t *testing.T, input string) tenantcapabilities.Entry {
	parts := strings.Split(input, ";")
	require.Equal(t, 2, len(parts))
	return tenantcapabilities.Entry{
		TenantID:           ParseTenantID(t, parts[0]),
		TenantCapabilities: parseTenantCapability(t, parts[1]),
	}
}

func parseTenantCapability(t *testing.T, input string) tenantcapabilitiespb.TenantCapabilities {
	var cap = tenantcapabilitiespb.TenantCapabilities{}
	err := json.Unmarshal([]byte(input), &cap)
	require.NoError(t, err)
	return cap
}

func ParseTenantID(t *testing.T, input string) roachpb.TenantID {
	if !tenIDRe.MatchString(input) {
		t.Fatalf("expected %s to match tenant ID regex", input)
	}
	matches := tenIDRe.FindStringSubmatch(input)

	if matches[3] == "system" {
		return roachpb.SystemTenantID
	}

	tenID, err := strconv.Atoi(matches[2])
	require.NoError(t, err)
	return roachpb.MustMakeTenantID(uint64(tenID))
}

func printTenantID(id roachpb.TenantID) string {
	return fmt.Sprintf("{ten=%s}", id)
}

func PrintTenantCapability(cap tenantcapabilitiespb.TenantCapabilities) string {
	bytes, err := json.Marshal(cap)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}
