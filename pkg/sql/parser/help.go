// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// HelpMessage describes a contextual help message.
type HelpMessage struct {
	// Command is set if the message is about a statement.
	Command string
	// Function is set if the message is about a built-in function.
	Function string

	// HelpMessageBody contains the details of the message.
	HelpMessageBody
}

// String implements the fmt.String interface.
func (h *HelpMessage) String() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "help: ")
	h.Format(&buf)
	return buf.String()
}

// Format prints out details about the message onto the specified output stream.
func (h *HelpMessage) Format(w io.Writer) {
	if h.Command != "" {
		fmt.Fprintf(w, "Command:     %s\n", h.Command)
	}
	if h.Function != "" {
		fmt.Fprintf(w, "Function:    %s\n", h.Function)
	}
	if h.ShortDescription != "" {
		fmt.Fprintf(w, "Description: %s\n", h.ShortDescription)
	}
	if h.Category != "" {
		fmt.Fprintf(w, "Category:    %s\n", h.Category)
	}
	if h.Command != "" {
		fmt.Fprintln(w, "Syntax:")
	}
	fmt.Fprintln(w, strings.TrimSpace(h.Text))
	if h.SeeAlso != "" {
		fmt.Fprintf(w, "\nSee also:\n  %s\n", h.SeeAlso)
	}
}

// helpWith is to be used in parser actions to mark the parser "in
// error", with the error set to a contextual help message about the
// current statement.
func helpWith(sqllex sqlLexer, helpText string) int {
	msg := HelpMessage{Command: helpText, HelpMessageBody: HelpMessages[helpText]}
	sqllex.(*Scanner).SetHelp(msg)
	// We return non-zero to indicate to the caller of Parse() that the
	// parse was unsuccesful.
	return 1
}

// helpWithFunction is to be used in parser actions to mark the parser
// "in error", with the error set to a contextual help message about
// the current built-in function.
func helpWithFunction(sqllex sqlLexer, f ResolvableFunctionReference) int {
	d, err := f.Resolve(nil)
	if err != nil {
		return 1
	}

	msg := HelpMessage{
		Function: f.String(),
		HelpMessageBody: HelpMessageBody{
			Category: "built-in functions",
			SeeAlso:  `https://www.cockroachlabs.com/docs/functions-and-operators.html`,
		},
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', 0)

	// Each function definition contains one or more overloads. We need
	// to extract them all; moreover each overload may have a different
	// documentation, so we need to also combine the descriptions
	// together.
	lastInfo := ""
	for i, overload := range d.Definition {
		b := overload.(Builtin)
		if b.Info != "" && b.Info != lastInfo {
			if i > 0 {
				fmt.Fprintln(w, "---")
			}
			fmt.Fprintf(w, "\n%s\n\n", b.Info)
			fmt.Fprintln(w, "Signature\tCategory")
		}
		lastInfo = b.Info

		cat := b.Category()
		if cat != "" {
			cat = "[" + cat + "]"
		}
		fmt.Fprintf(w, "%s%s\t%s\n", d.Name, b.Signature(), cat)
	}
	_ = w.Flush()
	msg.Text = buf.String()

	sqllex.(*Scanner).SetHelp(msg)
	return 1
}

const (
	hGroup = ""
	hDDL   = "schema manipulation"
	hDML   = "data manipulation"
	hTxn   = "transaction control"
	hPriv  = "privileges and security"
	hMisc  = "miscellaneous"
	hCfg   = "configuration"
	hCCL   = "enterprise features"
)

// HelpMessageBody defines the body of a help text. The messages are
// structured to facilitate future help navigation functionality.
type HelpMessageBody struct {
	Category         string
	ShortDescription string
	Text             string
	SeeAlso          string
}

// HelpMessages is the registry of all help messages, keyed by the
// top-level statement that they document. The key is intended for use
// via a (subsequent) \h client-side command.
var HelpMessages = func(h map[string]HelpMessageBody) map[string]HelpMessageBody {
	appendSeeAlso := func(newItem, prevItems string) string {
		// "See also" items start with no indentation, and then use two
		// space indentation from the 2nd item onward.
		if prevItems != "" {
			return newItem + "\n  " + prevItems
		}
		return newItem
	}
	srcMsg := h["<source>"]
	selectMsg := h["<selectclause>"]
	for k, m := range h {
		m = h[k]
		// If the description contains <source>, append the <source> help.
		if strings.Contains(m.Text, "<source>") && k != "<source>" {
			m.Text = strings.TrimSpace(m.Text) + "\n\n" + strings.TrimSpace(srcMsg.Text)
			m.SeeAlso = appendSeeAlso(srcMsg.SeeAlso, m.SeeAlso)
		}
		// Ditto for <selectclause>.
		if strings.Contains(m.Text, "<selectclause>") && k != "<selectclause>" {
			m.Text = strings.TrimSpace(m.Text) + "\n\n" + strings.TrimSpace(selectMsg.Text)
			m.SeeAlso = appendSeeAlso(selectMsg.SeeAlso, m.SeeAlso)
		}
		// If the description contains <tablename>, mention SHOW TABLES in "See Also".
		if strings.Contains(m.Text, "<tablename>") {
			m.SeeAlso = appendSeeAlso("SHOW TABLES", m.SeeAlso)
		}
		h[k] = m
	}
	return h
}(map[string]HelpMessageBody{
	"ALTER": {hGroup, ``, `ALTER TABLE, ALTER INDEX, ALTER DATABASE, ALTER VIEW`, ``},

	"ALTER TABLE": {hDDL, "change the definition of a table", `
ALTER TABLE [IF EXISTS] <tablename> <command> [, ...]

Commands:
  ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <colname> <type> [<qualifiers...>]
  ALTER TABLE ... ADD <constraint>
  ALTER TABLE ... DROP [COLUMN] [IF EXISTS] <colname> [RESTRICT | CASCADE]
  ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] <constraintname> [RESTRICT | CASCADE]
  ALTER TABLE ... ALTER [COLUMN] <colname> {SET DEFAULT <expr> | DROP DEFAULT}
  ALTER TABLE ... ALTER [COLUMN] <colname> DROP NOT NULL
  ALTER TABLE ... RENAME TO <newname>
  ALTER TABLE ... RENAME [COLUMN] <colname> TO <newname>
  ALTER TABLE ... VALIDATE CONSTRAINT <constraintname>
  ALTER TABLE ... SPLIT AT <selectclause>
  ALTER TABLE ... TESTING_RELOCATE <selectclause>
  ALTER TABLE ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]

Column qualifiers:
  [CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr>}
  FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
  REFERENCES <tablename> [( <colnames...> )]
  COLLATE <collationname>
`,
		`https://www.cockroachlabs.com/docs/alter-table.html`,
	},

	"ALTER INDEX": {hDDL, "change the definition of an index", `
ALTER INDEX [IF EXISTS] <idxname> <command>

Commands:
  ALTER INDEX ... RENAME TO <newname>
  ALTER INDEX ... SPLIT AT <selectclause>
  ALTER INDEX ... TESTING_RELOCATE <selectclause>
  ALTER INDEX ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]
`,
		`https://www.cockroachlabs.com/docs/alter-index.html`,
	},

	"ALTER DATABASE": {hDDL, "change a database",
		`ALTER DATABASE <name> RENAME TO <newname>`,
		`https://www.cockroachlabs.com/docs/alter-database.html`},

	"ALTER VIEW": {hDDL, "change the definition of a view",
		`ALTER VIEW [IF EXISTS] <name> RENAME TO <newname>`,
		`https://www.cockroachlabs.com/docs/alter-view.html`},

	"CREATE": {hGroup, ``, `CREATE DATABASE, CREATE TABLE, CREATE INDEX, CREATE TABLE AS, CREATE USER, CREATE VIEW`, ``},

	"CREATE DATABASE": {hDDL, "create a new database",
		`CREATE DATABASE [IF NOT EXISTS] <name>`,
		`https://www.cockroachlabs.com/docs/create-database.html`,
	},

	"CREATE INDEX": {hDDL, "create a new index", `
CREATE [UNIQUE] INDEX [IF NOT EXISTS] [<idxname>]
       ON <tablename> ( <colname> [ASC | DESC] [, ...] )
       [STORING ( <colnames...> )] [<interleave>]

Interleave clause:
   INTERLEAVE IN PARENT <tablename> ( <colnames...> ) [CASCADE | RESTRICT]
`,
		`https://www.cockroachlabs.com/docs/create-index.html`,
	},

	"CREATE USER": {hPriv, "define a new user",
		`CREATE USER <name> [ [WITH] PASSWORD <passwd> ]`,
		`https://www.cockroachlabs.com/docs/create-user.html`,
	},

	"CREATE VIEW": {hDDL, "create a new view",
		`CREATE VIEW <viewname> [( <colnames...> )] AS <source>`,
		`https://www.cockroachlabs.com/docs/create-view.html`,
	},

	"CREATE TABLE": {hDDL, "create a new table", `
CREATE TABLE [IF NOT EXISTS] <tablename> ( <elements...> ) [<interleave>]
CREATE TABLE [IF NOT EXISTS] <tablename> [( <colnames...> )] AS <source>

Table elements:
   <name> <type> [<qualifiers...>]
   [UNIQUE] INDEX [<name>] ( <colname> [ASC | DESC] [, ...] )
                           [STORING ( <colnames...> )] [<interleave>]
   FAMILY [<name>] ( <colnames...> )
   [CONSTRAINT <name>] <constraint>

Table constraints:
   PRIMARY KEY ( <colnames...> )
   FOREIGN KEY ( <colnames...> ) REFERENCES <tablename> [( <colnames...> )]
   UNIQUE ( <colnames... ) [STORING ( <colnames...> )] [<interleave>]
   CHECK ( <expr> )

Column qualifiers:
  [CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr>}
  FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
  REFERENCES <tablename> [( <colnames...> )]
  COLLATE <collationname>

Interleave clause:
   INTERLEAVE IN PARENT <tablename> ( <colnames...> ) [CASCADE | RESTRICT]
`,
		`https://www.cockroachlabs.com/docs/create-table.html
  https://www.cockroachlabs.com/docs/create-table-as.html`,
	},

	"DELETE": {hDML, "delete rows of a table",
		`DELETE FROM <tablename> [WHERE <expr>] [RETURNING <exprs...>]`,
		`https://www.cockroachlabs.com/docs/delete.html`,
	},

	"DROP": {hGroup, ``, `DROP DATABASE, DROP INDEX, DROP TABLE, DROP VIEW`, ``},

	"DROP DATABASE": {hDDL, "remove a database",
		`DROP DATABASE [IF EXISTS] <databasename>`,
		`https://www.cockroachlabs.com/docs/drop-database.html`,
	},

	"DROP INDEX": {hDDL, "remove an index",
		`DROP INDEX [IF EXISTS] <idxname> [, ...] [CASCADE | RESTRICT]`,
		`https://www.cockroachlabs.com/docs/drop-index.html`,
	},

	"DROP TABLE": {hDDL, "remove a table",
		`DROP TABLE [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]`,
		`https://www.cockroachlabs.com/docs/drop-table.html`,
	},

	"DROP VIEW": {hDDL, "remove a view",
		`DROP VIEW [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]`,
		`https://www.cockroachlabs.com/docs/drop-view.html`,
	},

	"EXPLAIN": {hMisc, "show the logical plan of a query", `
EXPLAIN <statement>
EXPLAIN [( [PLAN ,] <planoptions...> )] <statement>

Explainable statements: SELECT, CREATE, DROP, ALTER, INSERT, UPSERT, UPDATE, DELETE, SHOW, HELP, EXPLAIN
Plan options:           TYPES, EXPRS, METADATA, QUALIFY, INDENT, VERBOSE
`,
		`https://www.cockroachlabs.com/docs/explain.html`,
	},

	"INSERT": {hDML, "create new rows in a table", `
INSERT INTO <tablename> [[AS] <name>] [( <colnames...> )]
       <selectclause>
       [ON CONFLICT [( <colnames...> )] {DO UPDATE SET ... [WHERE <expr>] | DO NOTHING}]
       [RETURNING <exprs...>]
`,
		`UPSERT
  UPDATE
  DELETE
  https://www.cockroachlabs.com/docs/insert.html`,
	},

	"UPSERT": {hDML, "create or replace rows in a table", `
UPSERT INTO <tablename> [AS <name>] [( <colnames...> )]
       <selectclause>
       [RETURNING <exprs...>]
`,
		`INSERT
  UPDATE
  DELETE
  https://www.cockroachlabs.com/docs/upsert.html`,
	},

	"UPDATE": {hDML, "update rows of a table",
		`UPDATE <tablename> [[AS] <name>] SET ... [WHERE <expr>] [RETURNING <exprs...>]`,
		`INSERT
  UPSERT
  DELETE
  https://www.cockroachlabs.com/docs/update.html`,
	},

	"GRANT": {hPriv, "define access privileges", `
GRANT {ALL | <privileges...> } ON <targets...> TO <grantees...>

Privileges:
  CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE

Targets:
  DATABASE <databasename> [, <databasename>]...
  [TABLE] [<databasename> .] { <tablename> | * } [, ...]
`,
		`REVOKE
  https://www.cockroachlabs.com/docs/grant.html`,
	},

	"REVOKE": {hPriv, "remove access privileges", `
REVOKE {ALL | <privileges...> } ON <targets...> FROM <grantees...>

Privileges:
  CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE

Targets:
  DATABASE <databasename> [, <databasename>]...
  [TABLE] [<databasename> .] { <tablename> | * } [, ...]
`,
		`GRANT
  https://www.cockroachlabs.com/docs/revoke.html`,
	},

	"SHOW": {hGroup, ``,
		`SHOW SESSION, SHOW CLUSTER SETTING, SHOW DATABASES, SHOW TABLES, SHOW COLUMNS, SHOW INDEXES,\n` +
			`SHOW CONSTRAINTS, SHOW CREATE TABLE, SHOW CREATE VIEW, SHOW USERS, SHOW TRANSACTION, SHOW BACKUP`, ``},

	"SHOW CLUSTER SETTING": {hCfg, "display cluster settings", `
SHOW CLUSTER SETTING <var>
SHOW ALL CLUSTER SETTINGS
`, `https://www.cockroachlabs.com/docs/cluster-settings.html`},

	"SHOW BACKUP":       {hCCL, "list backup contents", `SHOW BACKUP <location>`, `https://www.cockroachlabs.com/docs/show-backup.html`},
	"SHOW COLUMNS":      {hDDL, "list columns", `SHOW COLUMNS FROM <tablename>`, `https://www.cockroachlabs.com/docs/show-columns.html`},
	"SHOW CONSTRAINTS":  {hDDL, "list constraints", `SHOW CONSTRAINTS FROM <tablename>`, `https://www.cockroachlabs.com/docs/show-constraints.html`},
	"SHOW CREATE TABLE": {hDDL, "show the CREATE TABLE statement for a table", `SHOW CREATE TABLE <tablename>`, `https://www.cockroachlabs.com/docs/show-create-table.html`},
	"SHOW CREATE VIEW":  {hDDL, "show the CREATE VIEW statement for a view", `SHOW CREATE VIEW <viewname>`, `https://www.cockroachlabs.com/docs/show-create-view.html`},
	"SHOW DATABASES":    {hDDL, "list databases", `SHOW DATABASES`, `https://www.cockroachlabs.com/docs/show-databases.html`},
	"SHOW GRANTS":       {hPriv, "list grants", `SHOW GRANTS [ON <targets...>] [FOR <users...>]`, `https://www.cockroachlabs.com/docs/show-grants.html`},
	"SHOW INDEXES":      {hDDL, "list indexes", `SHOW INDEXES FROM <tablename>`, `https://www.cockroachlabs.com/docs/show-indexes.html`},
	"SHOW SESSION":      {hCfg, "display session variables", `SHOW [SESSION] { <var> | ALL }`, `https://www.cockroachlabs.com/docs/show-all.html`},
	"SHOW TABLES":       {hDDL, "list tables", `SHOW TABLES [FROM <databasename>]`, `https://www.cockroachlabs.com/docs/show-tables.html`},
	"SHOW TRANSACTION":  {hTxn, "display current transaction properties", `SHOW TRANSACTION {ISOLATION LEVEL | PRIORITY | STATUS}`, `https://www.cockroachlabs.com/docs/show-transaction.html`},
	"SHOW USERS":        {hPriv, "list defined users", `SHOW USERS`, `https://www.cockroachlabs.com/docs/show-users.html`},

	"TRUNCATE": {hDML, "empty one or more tables", `TRUNCATE [TABLE] <tablename> [, ...] [CASCADE | RESTRICT]`, `https://www.cockroachlabs.com/docs/truncate.html`},

	"SELECT": {hDML, "retrieve rows from a data source and compute a result", `
SELECT [DISTINCT]
       { <expr> [[AS] <name>] | [ [<dbname>.] <tablename>. ] * } [, ...]
       [ FROM <source> ]
       [ WHERE <expr> ]
       [ GROUP BY <expr> [,...] ]
       [ HAVING <expr> ]
       [ WINDOW <name> AS ( <definition> ) ]
       [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] <select> ]
       [ ORDER BY <expr> [ ASC | DESC ] [, ...] ]
       [ LIMIT { <expr> | ALL } ]
       [ OFFSET <expr> [ ROW | ROWS ] ]
`,
		`https://www.cockroachlabs.com/docs/select.html`,
	},

	"<source>": {hDML, "define a data source for SELECT", `
Data sources:
  <tablename> [ @ { <idxname> | <indexhint> } ]
  <tablefunc> ( <exprs...> )
  ( { <selectclause> | <source> } )
  <source> [AS] <alias> [( <colnames...> )]
  <source> { [INNER] | { LEFT | RIGHT } [OUTER] } JOIN <source> ON <expr>
  <source> { [INNER] | { LEFT | RIGHT } [OUTER] } JOIN <source> USING ( <colnames...> )
  <source> NATURAL { [INNER] | { LEFT | RIGHT } [OUTER] } JOIN <source>
  <source> CROSS JOIN <source>
  <source> WITH ORDINALITY
  '[' EXPLAIN ... ']'
  '[' SHOW ... ']'

Index hints:
  '{' FORCE_INDEX = <idxname> [, ...] '}'
  '{' NO_INDEX_JOIN [, ...] '}'
`,
		`https://www.cockroachlabs.com/docs/table-expressions.html`,
	},

	"<selectclause>": {hDML, `access tabular data`, `
Select clause:
  TABLE <tablename>
  VALUES ( <exprs...> ) [ , ... ]
  SELECT ...
`,
		``},

	"TABLE": {hDML, "select an entire table",
		`TABLE <tablename>`,
		`SELECT
  VALUES
  https://www.cockroachlabs.com/docs/table-expressions.html`},

	"VALUES": {hDML, "select a given set of values",
		`VALUES ( <exprs...> ) [, ...]`,
		`TABLE
  SELECT
  https://www.cockroachlabs.com/docs/table-expressions.html`},

	"RESET": {hCfg, "reset a session variable to its default",
		`RESET [SESSION] <var>`,
		`https://www.cockroachlabs.com/docs/set-vars.html`},

	"SET SESSION": {hCfg, "change a session variable", `
SET [SESSION] <var> { TO | = } <values...>
SET [SESSION] TIME ZONE <tz>
SET [SESSION] CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
`, `SET CLUSTER SETTING
  SET TRANSACTION
  https://www.cockroachlabs.com/docs/set-vars.html`},

	"SET CLUSTER SETTING": {hCfg, "change a cluster setting", `
SET CLUSTER SETTING <var> { TO | = } <value>
`, `SET SESSION
  https://www.cockroachlabs.com/docs/cluster-settings.html`},

	"SET TRANSACTION": {hTxn, "configure the transaction settings", `
SET [SESSION] TRANSACTION <txnparameters...>

Transaction parameters:
   ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
   PRIORITY { LOW | NORMAL | HIGH }
`, `SET SESSION
  https://www.cockroachlabs.com/docs/set-transaction.html`,
	},

	"BEGIN": {hTxn, "start a transaction", `
BEGIN [TRANSACTION] [ <txnparameter> [, ...] ]
START TRANSACTION [ <txnparameter> [, ...] ]

Transaction parameters:
   ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }
   PRIORITY { LOW | NORMAL | HIGH }
`,
		`COMMIT
  ROLLBACK
  https://www.cockroachlabs.com/docs/begin-transaction.html`,
	},

	"COMMIT": {hTxn, "commit the current transaction", `
COMMIT [TRANSACTION]
END [TRANSACTION]
`,
		`BEGIN
  ROLLBACK
  https://www.cockroachlabs.com/docs/commit-transaction.html`,
	},

	"ROLLBACK": {hTxn, "abort the current transaction",
		`ROLLBACK [TRANSACTION] [TO [SAVEPOINT] cockroach_restart]`,
		`BEGIN
  COMMIT
  https://www.cockroachlabs.com/docs/rollback-transaction.html`,
	},

	"SAVEPOINT": {hTxn, "start a retryable block",
		`SAVEPOINT cockroach_restart`,
		`RELEASE
  https://www.cockroachlabs.com/docs/savepoint.html`},

	"RELEASE": {hTxn, "complete a retryable block",
		`RELEASE [SAVEPOINT] cockroach_restart`,
		`SAVEPOINT
  https://www.cockroachlabs.com/docs/savepoint.html`,
	},

	"PREPARE": {hMisc, "prepare a statement for later execution",
		`PREPARE <name> [ ( <types...> ) ] AS <query>`,
		`EXECUTE
  DEALLOCATE`},

	"EXECUTE": {hMisc, "execute a statement prepared previously",
		`EXECUTE <name> [ ( <exprs...> ) ]`,
		`PREPARE
  DEALLOCATE`},

	"DEALLOCATE": {hMisc, "remove a prepared statement",
		`DEALLOCATE [PREPARE] { <name> | ALL }`,
		`PREPARE
  EXECUTE`},

	"BACKUP": {hCCL, "back up data to external storage", `
BACKUP <targets...> TO <location...>
       [ AS OF SYSTEM TIME <expr> ]
       [ INCREMENTAL FROM <location...> ]
       [ WITH OPTIONS ( <options...> ) ]

Targets:
   TABLE <pattern> [ , <patterns...> ]
   DATABASE <databasename> [ , <databasenames...> ]

Location:
   "[scheme]://[host]/[path to backup]?[parameters]"

Options:
   INTO_DB
   SKIP_MISSING_FOREIGN_KEYS

`, `RESTORE
  https://www.cockroachlabs.com/docs/backup.html`},

	"RESTORE": {hCCL, "back up data to external storage", `
RESTORE <targets...> FROM <location...>
        [ AS OF SYSTEM TIME <expr> ]
        [ WITH OPTIONS ( <options...> ) ]

Targets:
   TABLE <pattern> [ , <patterns...> ]
   DATABASE <databasename> [ , <databasenames...> ]

Locations:
   "[scheme]://[host]/[path to backup]?[parameters]"

Options:
   INTO_DB
   SKIP_MISSING_FOREIGN_KEYS

`, `BACKUP
  https://www.cockroachlabs.com/docs/restore.html`},
})
