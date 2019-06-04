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
import { EventInfo, getDroppedObjectsText } from "src/util/events";

describe("getDroppedObjectsText", function() {

  // The key indicating which objects were dropped in a DROP_DATABASE event has been
  // renamed multiple times, creating bugs (e.g. #18523). This test won't fail if the
  // key is renamed again on the Go side, but it at least tests that we can handle all
  // existing versions.
  it("returns a sentence for all versions of the dropped objects key", function() {
    const commonProperties: EventInfo = {
      User: "root",
      DatabaseName: "foo",
    };
    const versions: EventInfo[] = [
      {
        ...commonProperties,
        DroppedTables: ["foo", "bar"],
      },
      {
        ...commonProperties,
        DroppedTablesAndViews: ["foo", "bar"],
      },
      {
        ...commonProperties,
        DroppedSchemaObjects: ["foo", "bar"],
      },
    ];

    const expected = "2 schema objects were dropped: foo, bar";

    versions.forEach((eventInfoVersion) => {
      assert.equal(expected, getDroppedObjectsText(eventInfoVersion));
    });
  });

});
