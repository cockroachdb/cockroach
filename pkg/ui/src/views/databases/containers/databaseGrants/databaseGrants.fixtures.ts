// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const mockedActions = {
  setSort: () => {},
  refreshDatabaseDetails: () => {},
};

export const loadingProps: any = {
  ...mockedActions,
  name: "system",
  dbResponse: { inFlight: true },
};

export const emptyProps: any = {
  ...mockedActions,
  name: "system",
  dbResponse: { inFlight: false },
};

export const fullfilledProps: any = {
  ...mockedActions,
  name: "system",
  dbResponse: { inFlight: false },
  grants: [
    { user: "admin", privileges: ["GRANT"] },
    { user: "admin", privileges: ["SELECT"] },
    { user: "root", privileges: ["GRANT"] },
    { user: "root", privileges: ["SELECT"] },
  ],
};
