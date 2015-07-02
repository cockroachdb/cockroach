// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import (
	"database/sql"
	"database/sql/driver"
	"net/url"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
)

func init() {
	sql.Register("cockroach", &roachDriver{})
}

// roachDriver implements the database/sql/driver.Driver interface. Named
// roachDriver so as not to conflict with the "driver" package name.
type roachDriver struct{}

func (d *roachDriver) Open(dsn string) (driver.Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	ctx := &base.Context{}
	ctx.InitDefaults()
	if u.User != nil {
		ctx.User = u.User.Username()
	}
	q := u.Query()
	if dir := q["certs"]; len(dir) > 0 {
		ctx.Certs = dir[0]
	}

	sender, err := newHTTPSender(u.Host, ctx, client.DefaultTxnRetryOptions)
	if err != nil {
		return nil, err
	}
	return &conn{sender: sender}, nil
}
