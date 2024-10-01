// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";

import {
  EventInfo,
  getDroppedObjectsText,
  getEventDescription,
} from "src/util/events";

describe("getDroppedObjectsText", function () {
  // The key indicating which objects were dropped in a DROP_DATABASE event has been
  // renamed multiple times, creating bugs (e.g. #18523). This test won't fail if the
  // key is renamed again on the Go side, but it at least tests that we can handle all
  // existing versions.
  it("returns a sentence for all versions of the dropped objects key", function () {
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

    versions.forEach(eventInfoVersion => {
      expect(expected).toEqual(getDroppedObjectsText(eventInfoVersion));
    });
  });
});

describe("getEventDescription", function () {
  it("ignores the options field when empty for role changes", function () {
    interface TestCase {
      event: Partial<clusterUiApi.EventColumns>;
      expected: string;
    }
    const tcs: TestCase[] = [
      {
        event: {
          eventType: "alter_role",
          info: '{"User": "abc", "RoleName": "123"}',
        },
        expected: "Role Altered: User abc altered role 123",
      },
      {
        event: {
          eventType: "alter_role",
          info: '{"User": "abc", "RoleName": "123", "SetInfo": ["DEFAULTSETTINGS"]}',
        },
        expected:
          "Role Altered: User abc altered default settings for role 123",
      },
      {
        event: {
          eventType: "alter_role",
          info: '{"User": "abc", "RoleName": "123", "Options": []}',
        },
        expected: "Role Altered: User abc altered role 123",
      },
      {
        event: {
          eventType: "alter_role",
          info: '{"User": "abc", "RoleName": "123", "Options": ["o1", "o2"]}',
        },
        expected: "Role Altered: User abc altered role 123 with options o1,o2",
      },
    ];
    tcs.forEach(tc => {
      expect(
        getEventDescription(tc.event as clusterUiApi.EventColumns),
      ).toEqual(tc.expected);
    });
  });
});
