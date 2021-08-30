// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import assert from "assert";
import { History, createMemoryHistory } from "history";
import { QueryParams } from "./queryParams";

describe("SortedTable/QueryParams", function() {
  let subject: QueryParams;
  let history: History;

  beforeEach(function() {
    history = createMemoryHistory();
    subject = new QueryParams(history);
  });

  describe("pagination", function() {
    describe("reading", function() {
      it("defaults to page 1", function() {
        history.location.search = "";
        assert.deepStrictEqual(subject.pagination, {
          current: 1,
          pageSize: 20,
        });
      });

      it("respects the page number in the url", function() {
        history.location.search = "?page=2";
        assert.deepStrictEqual(subject.pagination, {
          current: 2,
          pageSize: 20,
        });
      });

      it("falls back to page 1 for bogus values", function() {
        history.location.search = "?page=BOGUS";
        assert.deepStrictEqual(subject.pagination, {
          current: 1,
          pageSize: 20,
        });
      });
    });

    describe("writing", function() {
      it("puts a page parameter in the query string", function() {
        history.location.search = "";
        subject.pagination = { current: 2, pageSize: 20 };
        assert.deepStrictEqual(history.location.search, "?page=2");
      });

      it("clears the page parameter in the query string for page 1", function() {
        history.location.search = "?page=2";
        subject.pagination = { current: 1, pageSize: 20 };
        assert.deepStrictEqual(history.location.search, "");
      });

      it("uses history.push", function() {
        subject.pagination = { current: 2, pageSize: 20 };
        assert.deepStrictEqual(history.action, "PUSH");
      });
    });
  });

  describe("sortSetting", function() {
    describe("reading", function() {
      it("defaults to no column, ascending", function() {
        history.location.search = "";
        assert.deepStrictEqual(subject.sortSetting, {
          ascending: true,
          columnTitle: null,
        });
      });

      it("respects a descending parameter in the url", function() {
        history.location.search = "?descending=true";
        assert.deepStrictEqual(subject.sortSetting.ascending, false);
      });

      it("respects a sortBy parameter in the url", function() {
        history.location.search = "?sortBy=foo";
        assert.deepStrictEqual(subject.sortSetting.columnTitle, "foo");
      });
    });

    describe("writing", function() {
      it("puts descending and sortBy parameters in the query string", function() {
        history.location.search = "";
        subject.sortSetting = { ascending: false, columnTitle: "foo" };
        assert.deepStrictEqual(
          history.location.search,
          "?descending=true&sortBy=foo",
        );
      });

      it("clears the descending parameter in the query string when ascending", function() {
        history.location.search = "?descending=true";
        subject.sortSetting = { ascending: true };
        assert.deepStrictEqual(history.location.search, "");
      });

      it("clears the sortBy parameter in the query string when no columnTitle is given", function() {
        history.location.search = "?sortBy=foo";
        subject.sortSetting = { ascending: true };
        assert.deepStrictEqual(history.location.search, "");
      });

      it("uses history.replace", function() {
        subject.sortSetting = { ascending: false, columnTitle: "foo" };
        assert.deepStrictEqual(history.action, "REPLACE");
      });
    });
  });
});
