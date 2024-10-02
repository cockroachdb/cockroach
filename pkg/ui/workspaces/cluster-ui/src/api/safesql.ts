// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// SQLStringer is an interface that can be implemented by types to customize how
// they are formatted into SQL by Format.
export interface SQLStringer {
  sqlString: () => string;
}

export type SqlFormatArg = string | number | SQLStringer;

function isSQLStringer(s: unknown): s is SQLStringer {
  // Check that s is not null/undefined (truthy) and has SQLString implemented.
  return s && (s as SQLStringer).sqlString !== undefined;
}

// TODO(thomas): @knz says: the quoting rules for usernames are different from
// both strings and identifiers! So you might want to do something similar to
// crdb and make security.SQLUsername a special case here.

// Identifier provides proper quoting of SQL identifiers (such as column,
// table, and database names) when used as arguments to Format.
export class Identifier implements SQLStringer {
  i: string;
  constructor(identifier: string) {
    this.i = identifier;
  }

  // SQLString implements the SQLStringer interface.
  sqlString = (): string => {
    return QuoteIdentifier(this.i);
  };
}

// QualifiedIdentifier provides proper quoting of a qualified identifier
// with an arbitrary number of components (e.g. <schema>.<table>.<column>)
// when used as an argument to Format.
export class QualifiedIdentifier implements SQLStringer {
  qi: string[];
  constructor(identifiers: string[]) {
    this.qi = identifiers;
  }

  // SQLString implements the SQLStringer interface.
  sqlString = (): string => {
    const quotedParts = this.qi.map(iden => new Identifier(iden).sqlString());
    return quotedParts.join(".");
  };
}

// SQL encapsulates a known safe SQL fragment. It should not be used with SQL
// from an external source or SQL containing identifiers.
//
// Use of this type presents a security risk: the encapsulated content should
// come from a trusted source, as it will be included verbatim in the output
// when used as an argument to Format().
export class SQL implements SQLStringer {
  sql: string;
  constructor(sql: string) {
    this.sql = sql;
  }

  // SQLString implements the SQLStringer interface.
  sqlString = (): string => {
    return this.sql;
  };
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    quoted := pq.QuoteIdentifier(tblname)
//    err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
// Cribbed from https://github.com/lib/pq and Typescript-ified.
export function QuoteIdentifier(name: string): string {
  // Use a search regex to replace all occurrences instead of just the first occurrence.
  const search = /"/g;
  return `"` + name.replace(search, `""`) + `"`;
}

// QuoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement.  For example:
//
//    exp_date := pq.QuoteLiteral("2023-01-05 15:00:00Z")
//    err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
// Cribbed from https://github.com/lib/pq and Typescript-ified.
function QuoteLiteral(literal: string): string {
  // This follows the PostgreSQL internal algorithm for handling quoted literals
  // from libpq, which can be found in the "PQEscapeStringInternal" function,
  // which is found in the libpq/fe-exec.c source file:
  // https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
  //
  // substitute any single-quotes (') with two single-quotes ('')

  // Use a search regex to replace all occurrences instead of just the first occurrence.
  const search = /'/g;
  literal = literal.replace(search, `''`);
  // determine if the string has any backslashes (\) in it.
  // if it does, replace any backslashes (\) with two backslashes (\\)
  // then, we need to wrap the entire string with a PostgreSQL
  // C-style escape. Per how "PQEscapeStringInternal" handles this case, we
  // also add a space before the "E"
  if (literal.includes(`\\`)) {
    literal = literal.replace(`\\`, `\\\\`);
    literal = ` E'` + literal + `'`;
  } else {
    // otherwise, we can just wrap the literal with a pair of single quotes
    literal = `'` + literal + `'`;
  }
  return literal;
}

function parseArgIdx(s: string, i: number): [number, number] {
  if (i >= s.length || s[i] < "0" || s[i] > "9") {
    // Not a valid number.
    return [-1, i];
  }

  const end = s.length;
  let idx = 0;
  while (i < end && "0" < s[i] && s[i] <= "9") {
    if (idx >= 1e6) {
      // Too large an argument index specifier.
      return [-1, i];
    }
    idx = idx * 10 + (s.codePointAt(i) - "0".codePointAt(0));
    i++;
  }
  return [idx, i];
}

// Format formats a SQL string, recognizing %1, %2, etc as placeholders and
// replacing them with the corresponding arguments (1-indexed). Unlike
// printf-style formatting, the placeholders must be preceded by punctuation or
// whitespace. This prevents errors such as `xxx_%1` from being formatted as
// `xxx_'%1'` which is almost certainly incorrect.
//
// Formatting of the argument proceeds by the following rules:
//
// - if args[i] has a SQLString() method, it is used
// - if args[i] is a string type, it is quoted as a literal (pq.QuoteLiteral)
// - if args[i] is an integer type, it is formatted as %d
// - otherwise, panic with an error
export function Format(format: string, args?: SqlFormatArg[]): string {
  let resultString = "";
  // The loop structure here is adapted from the stdlib fmt.Sprintf code, but
  // heavily simplified because we don't have to support different verbs, only
  // formatting of different types.
  let i = 0;
  const end = format.length;
  while (i < end) {
    let start = i;
    while (i < end && format[i] !== "%") {
      i++;
    }
    if (start < i) {
      resultString += format.slice(start, i);
    }
    if (i >= end) {
      break;
    }

    if (i > 0) {
      const lastChar = format.substring(0, i).slice(-1);
      if (!(lastChar === "=" || isPunct(lastChar) || isSpace(lastChar))) {
        throw new Error(
          `invalid separator: '${lastChar}' is not punctuation or whitespace`,
        );
      }
    }

    start = i;
    i++;
    if (i < end && format[i] === "%") {
      i++;
      resultString += "%";
      continue;
    }

    let argIdx: number;
    [argIdx, i] = parseArgIdx(format, i);
    if (argIdx === -1) {
      throw new Error(`invalid placeholder: ${format.slice(start)}`);
    }
    // Check for:
    // - invalid argument index
    // - valid argument index but no arguments
    // - valid argument index but exceeds arguments length
    if (argIdx < 1 || !args || argIdx > args.length) {
      throw new Error(`bad placeholder index: ${format.slice(start)}`);
    }

    const arg = args[argIdx - 1];
    let errString: string;
    [resultString, errString] = writeFormattedArg(arg, resultString);
    if (errString !== "") {
      throw new Error(`bad argument ${argIdx}: ${errString}`);
    }
  }

  return resultString;
}

// Join concatenates the given elements with the given separator, formatting each element per Format.
// A type parameter is used here as we typically expect to see a slice of a concrete type passed as `args`
// and using one avoids the need to convert the slice.
export function Join<Type extends SqlFormatArg>(args: Type[], sep: SQL): SQL {
  let b = "";
  for (let i = 0; i < args.length; i++) {
    if (i > 0) {
      b += sep.sqlString();
    }
    let errString: string;
    [b, errString] = writeFormattedArg(args[i], b);
    if (errString !== "") {
      throw new Error(errString);
    }
  }
  return new SQL(b);
}

function writeFormattedArg(arg: SqlFormatArg, b: string): [string, string] {
  if (isSQLStringer(arg)) {
    b += arg.sqlString();
    return [b, ""];
  }
  const type = typeof arg;
  if (type === "number") {
    b += arg;
    return [b, ""];
  }
  if (type === "string") {
    b += QuoteLiteral(arg.toString());
    return [b, ""];
  }
  return ["", "unsupported type: " + type];
}

function isPunct(char: string): boolean {
  return !!char.match(/^[.,:!?]/);
}

function isSpace(char: string): boolean {
  switch (char) {
    case "\t":
    case "\n":
    case "\v":
    case "\f":
    case "\r":
    case " ":
    case String.fromCharCode(0x85):
    case String.fromCharCode(0xa0):
      return true;
    default:
      return false;
  }
}
