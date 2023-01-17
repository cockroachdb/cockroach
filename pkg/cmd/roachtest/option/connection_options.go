// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

type ConnOption struct {
	User       string
	TenantName string
}

func User(user string) func(*ConnOption) {
	return func(option *ConnOption) {
		option.User = user
	}
}

func TenantName(tenantName string) func(*ConnOption) {
	return func(option *ConnOption) {
		option.TenantName = tenantName
	}
}
