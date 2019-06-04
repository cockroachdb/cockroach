// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import { assert } from "chai";

import * as protos from "src/js/protos";
import { selectDatabasesByType } from "./";

describe("selectDatabasesByType", function() {
  it("returns empty arrays if database data is missing", function() {
    const state = {
      cachedData: {
        databases: {
          inFlight: false,
          valid: false,
        },
      },
    };

    assert.deepEqual(selectDatabasesByType(state), { user: [], system: [] });
  });

  it("separates out the system databases", function() {
    const userDatabases = ["foo", "bar", "baz"];
    const systemDatabases = ["defaultdb", "postgres", "system"];
    const state = {
      cachedData: {
        databases: {
          inFlight: false,
          valid: true,
          data: protos.cockroach.server.serverpb.DatabasesResponse.fromObject({
            databases: systemDatabases.concat(userDatabases),
          }),
        },
      },
    };

    const dbs = selectDatabasesByType(state);

    assert.deepEqual(dbs, { user: userDatabases, system: systemDatabases });
  });
});
