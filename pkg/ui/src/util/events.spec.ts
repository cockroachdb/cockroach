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
