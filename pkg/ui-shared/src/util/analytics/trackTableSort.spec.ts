// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { get, isString } from "lodash";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { track } from "./trackTableSort";

const sandbox = createSandbox();

describe("trackTableSort", () => {
  afterEach(() => {
    sandbox.reset();
  });

  it("should only call track once", () => {
    const spy = sandbox.spy();
    track(spy)();
    assert.isTrue(spy.calledOnce);
  });

  it("should send the right event", () => {
    const spy = sandbox.spy();
    const expected = "Table Sort";

    track(spy)();

    const sent = spy.getCall(0).args[0];
    const event = get(sent, "event");

    assert.isTrue(isString(event));
    assert.isTrue(event === expected);
  });

  it("should send the correct payload", () => {
    const spy = sandbox.spy();
    const tableName = "Test table";
    const column = { title: "test", cell: (x: number) => x };
    const sortSetting = { sortKey: "whatever", ascending: true };

    track(spy)(tableName, column, sortSetting);

    const sent = spy.getCall(0).args[0];
    const table = get(sent, "properties.tableName");
    const columnName = get(sent, "properties.columnName");
    const direction = get(sent, "properties.sortDirection");

    assert.isTrue(isString(table), "Table name is a string");
    assert.isTrue(table === tableName, "Table name matches given table name");

    assert.isTrue(isString(columnName), "Column name is a string");
    assert.isTrue(column.title === columnName, "Column name matches given column name");

    assert.isTrue(isString(direction), "Sort direction is a string");
    assert.isTrue(
      direction === "asc",
      "Sort direction matches the sort setting ascending property",
    );
  });
});
