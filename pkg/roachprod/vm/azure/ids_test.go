// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import "testing"

func TestParseAzureID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
		want      azureID
	}{
		{
			name:  "Valid VM",
			input: "/subscriptions/1234-abcd/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/n01",
			want: azureID{
				subscription:               "1234-abcd",
				resourceGroup:              "my-rg",
				provider:                   "Microsoft.Compute",
				resourceType:               "virtualMachines",
				resourceName:               "n01",
				childResourceTypeNamePairs: "",
			},
		},
		{
			name:  "Valid NIC",
			input: "/subscriptions/1234-abcd/resourceGroups/test-rg/providers/Microsoft.Network/networkInterfaces/test-nic01",
			want: azureID{
				subscription:               "1234-abcd",
				resourceGroup:              "test-rg",
				provider:                   "Microsoft.Network",
				resourceType:               "networkInterfaces",
				resourceName:               "test-nic01",
				childResourceTypeNamePairs: "",
			},
		},
		{
			name:  "Valid with child resource",
			input: "/subscriptions/1111/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/lb01/frontendIPConfigurations/ipconfig1",
			want: azureID{
				subscription:               "1111",
				resourceGroup:              "rg1",
				provider:                   "Microsoft.Network",
				resourceType:               "loadBalancers",
				resourceName:               "lb01",
				childResourceTypeNamePairs: "frontendIPConfigurations/ipconfig1",
			},
		},
		{
			name:      "Invalid - missing fields",
			input:     "/subscriptions/abcd/resourceGroups//providers/Microsoft.Compute/virtualMachines/vm1",
			expectErr: true,
		},
		{
			name:      "Invalid - not an Azure ID",
			input:     "/this/is/not/an/azure/id",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAzureID(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			// Compare fields
			if got.subscription != tt.want.subscription {
				t.Errorf("subscription: got %q, want %q", got.subscription, tt.want.subscription)
			}
			if got.resourceGroup != tt.want.resourceGroup {
				t.Errorf("resourceGroup: got %q, want %q", got.resourceGroup, tt.want.resourceGroup)
			}
			if got.provider != tt.want.provider {
				t.Errorf("provider: got %q, want %q", got.provider, tt.want.provider)
			}
			if got.resourceType != tt.want.resourceType {
				t.Errorf("resourceType: got %q, want %q", got.resourceType, tt.want.resourceType)
			}
			if got.resourceName != tt.want.resourceName {
				t.Errorf("resourceName: got %q, want %q", got.resourceName, tt.want.resourceName)
			}
			if got.childResourceTypeNamePairs != tt.want.childResourceTypeNamePairs {
				t.Errorf("childResourceTypeNamePairs: got %q, want %q", got.childResourceTypeNamePairs, tt.want.childResourceTypeNamePairs)
			}
			// Ensure String() reconstructs the same input
			if got.String() != tt.input {
				t.Errorf("String(): got %q, want %q", got.String(), tt.input)
			}
		})
	}
}
