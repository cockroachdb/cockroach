// Copyright 2018 The Cockroach Authors.
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
