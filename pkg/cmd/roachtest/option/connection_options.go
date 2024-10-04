// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

import (
	"fmt"
	"time"
)

type ConnOption struct {
	User        string
	DBName      string
	TenantName  string
	SQLInstance int
	Options     map[string]string
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

func ConnectionOption(key, value string) func(*ConnOption) {
	return func(option *ConnOption) {
		if len(option.Options) == 0 {
			option.Options = make(map[string]string)
		}
		option.Options[key] = value
	}
}

func ConnectTimeout(t time.Duration) func(*ConnOption) {
	sec := int64(t.Seconds())
	if sec < 1 {
		sec = 1
	}
	return ConnectionOption("connect_timeout", fmt.Sprintf("%d", sec))
}

func DBName(dbName string) func(*ConnOption) {
	return func(option *ConnOption) {
		option.DBName = dbName
	}
}
