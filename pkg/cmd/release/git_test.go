// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "testing"

func Test_bumpVersion(t *testing.T) {
	type args struct {
		version     string
		releaseType releaseTypeOpt
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "stable",
			args: args{
				version:     "v22.1.2",
				releaseType: releaseTypeStable,
			},
			want:    "v22.1.3",
			wantErr: false,
		},
		{
			name: "stable 2",
			args: args{
				version:     "v22.1.0",
				releaseType: releaseTypeStable,
			},
			want:    "v22.1.1",
			wantErr: false,
		},
		{
			name: "alpha 0",
			args: args{
				version:     "v22.1.0-alpha.0",
				releaseType: releaseTypeAlpha,
			},
			want:    "v22.1.0-alpha.1",
			wantErr: false,
		},
		{
			name: "alpha 3",
			args: args{
				version:     "v22.1.0-alpha.3",
				releaseType: releaseTypeAlpha,
			},
			want:    "v22.1.0-alpha.4",
			wantErr: false,
		},
		{
			name: "beta 2",
			args: args{
				version:     "v22.1.0-beta.2",
				releaseType: releaseTypeBeta,
			},
			want:    "v22.1.0-beta.3",
			wantErr: false,
		},
		{
			name: "alpha to beta",
			args: args{
				version:     "v22.1.0-alpha.3",
				releaseType: releaseTypeBeta,
			},
			want:    "v22.1.0-beta.1",
			wantErr: false,
		},
		{
			name: "rc 2",
			args: args{
				version:     "v22.1.0-rc.2",
				releaseType: releaseTypeRC,
			},
			want:    "v22.1.0-rc.3",
			wantErr: false,
		},
		{
			name: "alpha to rc",
			args: args{
				version:     "v22.1.0-alpha.3",
				releaseType: releaseTypeRC,
			},
			want:    "v22.1.0-rc.1",
			wantErr: false,
		},
		{
			name: "beta to rc",
			args: args{
				version:     "v22.1.0-beta.3",
				releaseType: releaseTypeRC,
			},
			want:    "v22.1.0-rc.1",
			wantErr: false,
		},
		{
			name: "rc to beta",
			args: args{
				version:     "v22.1.0-rc.33",
				releaseType: releaseTypeBeta,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "rc to alpha",
			args: args{
				version:     "v22.1.0-rc.44",
				releaseType: releaseTypeAlpha,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "beta to alpha",
			args: args{
				version:     "v22.1.0-beta.4",
				releaseType: releaseTypeAlpha,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bumpVersion(tt.args.version, tt.args.releaseType)
			if (err != nil) != tt.wantErr {
				t.Errorf("bumpVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("bumpVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bumpPrerelease(t *testing.T) {
	tests := []struct {
		prerelease string
		want       string
		wantErr    bool
	}{
		{prerelease: "alpha.0", want: "alpha.1", wantErr: false},
		{prerelease: "alpha.1", want: "alpha.2", wantErr: false},
		{prerelease: "alpha.22", want: "alpha.23", wantErr: false},
		{prerelease: "beta.0", want: "beta.1", wantErr: false},
		{prerelease: "beta.1", want: "beta.2", wantErr: false},
		{prerelease: "beta.5", want: "beta.6", wantErr: false},
		{prerelease: "rc.0", want: "rc.1", wantErr: false},
		{prerelease: "rc.59", want: "rc.60", wantErr: false},
		{prerelease: "rc", want: "", wantErr: true},
		{prerelease: "rc.11.12", want: "", wantErr: true},
		{prerelease: "rc.one", want: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.prerelease, func(t *testing.T) {
			got, err := bumpPrerelease(tt.prerelease)
			if (err != nil) != tt.wantErr {
				t.Errorf("bumpPrerelease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("bumpPrerelease() got = %v, want %v", got, tt.want)
			}
		})
	}
}
