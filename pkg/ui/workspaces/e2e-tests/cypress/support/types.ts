// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export enum SQLPrivilege {
  ADMIN = "ADMIN",
  VIEWACTIVITY = "VIEWACTIVITY",
  VIEWACTIVITYREDACTED = "VIEWACTIVITYREDACTED",
  NONE = "NONE",
}

export type User = {
  username: string;
  password: string;
  sqlPrivileges: SQLPrivilege[];
};
