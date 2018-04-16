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
