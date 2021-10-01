// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { detailedSummarize } from "src/util";
import { detailedShortStatement } from "src";
import { assert } from "chai";

describe("detailedSummarize", () => {
  describe("returns select statements formatted properly", () => {
    it("format simple select statements", () => {
      const statement = "SELECT * FROM table";
      const expected = "SELECT * FROM table";
      const summary = detailedSummarize(statement);
      assert.equal(detailedShortStatement(summary, statement), expected);
    });

    it("format select statements with multiple columns and tables", () => {
      const statement =
        "SELECT vehicles.id, rides.id, users.id FROM vehicles, users, user_promo_codes, rides WHERE vehicles.city='amsterdam'";
      const expected =
        "SELECT vehicles.id, ri... FROM vehicles, users, user_promo_co...";
      const summary = detailedSummarize(statement);
      assert.equal(detailedShortStatement(summary, statement), expected);
    });

    it("format select statements with joins", () => {
      const statement =
        "SELECT vehicles.id FROM vehicles JOIN users ON vehicles.id = users.id JOIN rides ON rides.id = users.id WHERE vehicles.city=‘amsterdam’ limit 1";
      const expected = "SELECT vehicles.id FROM vehicles JOIN users";
      const summary = detailedSummarize(statement);
      assert.equal(detailedShortStatement(summary, statement), expected);
    });

    it("format select statements with inner selects", () => {
      const statement =
        "SELECT (SELECT count(*) FROM system.jobs) AS j, num_running, s.* FROM system.scheduled_jobs AS s WHERE next_run < current_timestamp() ORDER BY random() LIMIT _ FOR UPDATE";
      const expected =
        "SELECT (SELECT FROM sy... FROM system.scheduled_jobs AS s";
      const summary = detailedSummarize(statement);
      assert.equal(detailedShortStatement(summary, statement), expected);
    });
  });
});
