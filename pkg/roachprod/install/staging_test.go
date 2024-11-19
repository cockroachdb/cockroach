// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

func TestURLsForApplication(t *testing.T) {
	type args struct {
		application string
		version     string
		os          string
		arch        string
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
			name: "cockroach linux arm64 sha",
			args: args{
				application: "cockroach",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "linux",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-arm64.563ea3967c98c67d47ede30d895c82315e4b1a77",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.linux-gnu-arm64.563ea3967c98c67d47ede30d895c82315e4b1a77.so",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.linux-gnu-arm64.563ea3967c98c67d47ede30d895c82315e4b1a77.so",
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
			name: "cockroach darwin arm64 sha",
			args: args{
				application: "cockroach",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "darwin",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.darwin-arm64.unsigned.563ea3967c98c67d47ede30d895c82315e4b1a77",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.darwin-arm64.unsigned.563ea3967c98c67d47ede30d895c82315e4b1a77.dylib",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.darwin-arm64.unsigned.563ea3967c98c67d47ede30d895c82315e4b1a77.dylib",
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
			name: "workload",
			args: args{
				application: "workload",
				version:     "563ea3967c98c67d47ede30d895c82315e4b1a77",
				os:          "linux",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/workload.linux-gnu-arm64.563ea3967c98c67d47ede30d895c82315e4b1a77",
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
			name: "cockroach linux FIPS latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "linux",
				arch:        "fips",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-amd64-fips.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.linux-gnu-amd64-fips.so.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.linux-gnu-amd64-fips.so.LATEST",
			},
		},
		{
			name: "cockroach linux arm64 latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "linux",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.linux-gnu-arm64.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.linux-gnu-arm64.so.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.linux-gnu-arm64.so.LATEST",
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
			name: "cockroach darwin arm64 latest",
			args: args{
				application: "cockroach",
				version:     "",
				os:          "darwin",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/cockroach.darwin-arm64.unsigned.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos.darwin-arm64.unsigned.dylib.LATEST",
				"https://storage.googleapis.com/cockroach-edge-artifacts-prod/cockroach/lib/libgeos_c.darwin-arm64.unsigned.dylib.LATEST",
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
			name: "release linux FIPS",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "linux",
				arch:        "fips",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.linux-amd64-fips.tgz",
			},
		},
		{
			name: "release linux arm64",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "linux",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.linux-arm64.tgz",
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
			name: "release darwin arm64",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "darwin",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-release-artifacts-prod/cockroach-v22.1.11.darwin-11.0-arm64.tgz",
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
			name: "customized linux",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "linux",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.linux-amd64.tgz",
			},
		},
		{
			name: "customized linux FIPS",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "linux",
				arch:        "fips",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.linux-amd64-fips.tgz",
			},
		},
		{
			name: "customized linux arm64",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "linux",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.linux-arm64.tgz",
			},
		},
		{
			name: "customized darwin",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "darwin",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.darwin-10.9-amd64.tgz",
			},
		},
		{
			name: "customized darwin arm64",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "darwin",
				arch:        "arm64",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.darwin-11.0-arm64.tgz",
			},
		},
		{
			name: "customized windows",
			args: args{
				application: "customized",
				version:     "v22.1.11",
				os:          "windows",
			},
			want: []string{
				"https://storage.googleapis.com/cockroach-customized-builds-artifacts-prod/cockroach-v22.1.11.windows-6.2-amd64.zip",
			},
		},
		{
			name: "unsupported arch 'arm63'",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "darwin",
				arch:        "arm63",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "unsupported arch 'x86'",
			args: args{
				application: "release",
				version:     "v22.1.11",
				os:          "linux",
				arch:        "x86",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "windows unsupported on arm64",
			args: args{
				application: "cockroach",
				os:          "windows",
				arch:        "arm64",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "windows unsupported with FIPS",
			args: args{
				application: "cockroach",
				os:          "windows",
				arch:        "fips",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "darwin unsupported with FIPS",
			args: args{
				application: "cockroach",
				os:          "darwin",
				arch:        "fips",
			},
			wantErr: true,
			want:    nil,
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
			got, err := URLsForApplication(tt.args.application, tt.args.version, tt.args.os, vm.CPUArch(tt.args.arch))
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
