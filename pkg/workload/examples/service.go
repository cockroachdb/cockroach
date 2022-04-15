// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package examples

import "github.com/cockroachdb/cockroach/pkg/workload"

type service struct{}

func init() {
	workload.Register(serviceMeta)
}

var serviceMeta = workload.Meta{
	Name: `service`,
	Description: `Service contains a simple service definition.

To enable, run:

  SET database = service;
  CREATE SERVICE msgbox RULES FROM TABLE msgbox_rules;

Then navigate your browser to the service endpoint at URL /msgbox.
`,
	Version:      `1.0.0`,
	PublicFacing: true,
	New:          func() workload.Generator { return service{} },
}

// Meta implements the Generator interface.
func (service) Meta() workload.Meta { return serviceMeta }

// Tables implements the Generator interface.
func (service) Tables() []workload.Table {
	return []workload.Table{
		{
			Name:   `messages`,
			Schema: `(num SERIAL, msg STRING)`,
		},
		{
			Name:   `msgbox_rules`,
			Schema: `(path STRING, http_method STRING, authn_method STRING, exec_method STRING, template STRING, next_path STRING)`,
			InitialRows: workload.Tuples(
				len(msgBoxRules),
				func(rowIdx int) []interface{} {
					return msgBoxRules[rowIdx]
				}),
		},
	}
}

var msgBoxRules = [...][]interface{}{
	{`/login`, ``, ``, `login-cookie`, ``, `/msgbox`},
	{`/logout`, ``, ``, `logout-cookie`, ``, `/loginform`},
	{`/loginform`, `GET`, `anonymous`, `scriggo`, `
<html><body>
<form action=/login method=post>
Username: <input type=text id=username name=username><br/>
Password: <input type=password id=password name=password><br/>
<input type="submit" value="Login">
</form>
</body></html>
`, ``},
	{`/msgbox`, `GET,POST`, `cookie`, `scriggo`, `
<html><body>
<h1>Hello {{ getUser() }}!</h1>

{%%
 msg := getInput("msg")
 if msg != "" {
    exec("INSERT INTO messages(msg) VALUES($1)", msg)
 }
%%}

{% for row in query("SELECT msg FROM messages ORDER BY num") %}
<p>{{ toText(row[0]) }}</p>
{% end for %}

<form action=/msgbox method=post>
<input type=text id=msg name=msg>
<input type="submit" value="Submit">
</form>

<p><a href="/logout">logout</a></p>
</body></html>
`, ``,
	},
}
