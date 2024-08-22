// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
