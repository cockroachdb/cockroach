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
