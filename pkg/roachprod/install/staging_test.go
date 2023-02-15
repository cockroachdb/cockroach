// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLsForApplication(t *testing.T) {
	type args struct {
		application string
		version     string
		os          string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "cockroach linux sha",
			args: args{
				application: "cockroach",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "linux",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.linux-gnu-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.so",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.linux-gnu-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.so",
			},
		},
		{
			name: "cockroach darwin sha",
			args: args{
				application: "cockroach",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "darwin",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.darwin-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.darwin-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.dylib",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.darwin-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.dylib",
			},
		},
		{
			name: "cockroach windows sha",
			args: args{
				application: "cockroach",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "windows",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.windows-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.exe",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.windows-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.dll",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.windows-amd64.563ea3967c98c67d47ede30d895c82315e4b1a77.dll",
			},
		},
		{
			name: "workload",
			args: args{
				application: "workload",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "linux",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/workload.563ea3967c98c67d47ede30d895c82315e4b1a77",
			},
		},
		{
			name: "cockroach linux latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "linux",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-amd64.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.linux-gnu-amd64.so.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.linux-gnu-amd64.so.LATEST",
			},
		},
		{
			name: "cockroach darwin latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "darwin",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.darwin-amd64.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.darwin-amd64.dylib.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.darwin-amd64.dylib.LATEST",
			},
		},
		{
			name: "cockroach windows latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "windows",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.windows-amd64.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.windows-amd64.dll.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.windows-amd64.dll.LATEST",
			},
		},
		{
			name: "release linux",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "linux",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.linux-amd64.tgz",
			},
		},
		{
			name: "release darwin",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "darwin",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.darwin-10.9-amd64.tgz",
			},
		},
		{
			name: "release windows",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "windows",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.windows-6.2-amd64.zip",
			},
		},
		{
			name: "something else",
			args: args{
				application: "ccloud",
				version:     "v22.1.11",
				os:          "windows",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := URLsForApplication(tt.args.application, tt.args.version, tt.args.os)
			if (err != nil) != tt.wantErr {
				t.Errorf("URLsForApplication() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			var gotStr []string
			for _, url := range got {
				gotStr = append(gotStr, url.String())
			}
			require.Equal(t, tt.want, gotStr)
		})
	}
}
