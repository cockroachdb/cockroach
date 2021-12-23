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
import { createMemoryHistory } from "history";
import Long from "long";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";
import {
  IndexDetailPageActions,
  IndexDetailsPageData,
  util,
} from "@cockroachlabs/cluster-ui";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import {
  databaseNameAttr,
  indexNameAttr,
  tableNameAttr,
} from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";
import { mapStateToProps, mapDispatchToProps } from "./redux";
import moment from "moment";
import { makeTimestamp } from "src/views/databases/utils";

function fakeRouteComponentProps(
  k1: string,
  v1: string,
  k2: string,
  v2: string,
  k3: string,
  v3: string,
): RouteComponentProps {
  return {
    history: createMemoryHistory(),
    location: {
      pathname: "",
      search: "",
      state: {},
      hash: "",
    },
    match: {
      params: {
        [k1]: v1,
        [k2]: v2,
        [k3]: v3,
      },
      isExact: true,
      path: "",
      url: "",
    },
  };
}

class TestDriver {
  private readonly actions: IndexDetailPageActions;
  private readonly properties: () => IndexDetailsPageData;

  constructor(
    store: Store<AdminUIState>,
    private readonly database: string,
    private readonly table: string,
    index: string,
  ) {
    this.actions = bindActionCreators(
      mapDispatchToProps,
      store.dispatch.bind(store),
    );
    this.properties = () =>
      mapStateToProps(
        store.getState(),
        fakeRouteComponentProps(
          databaseNameAttr,
          database,
          tableNameAttr,
          table,
          indexNameAttr,
          index,
        ),
      );
  }

  assertProperties(
    expected: IndexDetailsPageData,
    compareTimestamps: boolean = true,
  ) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      assert(
        this.properties().details.lastRead.isSame(expected.details.lastRead),
      );
      assert(
        this.properties().details.lastReset.isSame(expected.details.lastReset),
      );
    }
    // Assert objects without moments are equal.
    delete this.properties().details.lastRead;
    delete expected.details.lastRead;
    delete this.properties().details.lastReset;
    delete expected.details.lastReset;
    assert.deepStrictEqual(this.properties(), expected);
  }

  async refreshIndexStats() {
    return this.actions.refreshIndexStats(this.database, this.table);
  }
}

describe("Index Details Page", function() {
  let driver: TestDriver;

  beforeEach(function() {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "DATABASE",
      "TABLE",
      "INDEX",
    );
  });

  afterEach(function() {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", function() {
    driver.assertProperties(
      {
        databaseName: "DATABASE",
        tableName: "TABLE",
        indexName: "INDEX",
        details: {
          loading: false,
          loaded: false,
          createStatement: "",
          totalReads: 0,
          lastRead: moment(),
          lastReset: moment(),
        },
      },
      false,
    );
  });

  it("loads index stats", async function() {
    fakeApi.stubIndexStats("DATABASE", "TABLE", {
      statistics: [
        {
          statistics: {
            key: {
              table_id: 15,
              index_id: 2,
            },
            stats: {
              total_read_count: new Long(2),
              last_read: makeTimestamp("2021-11-19T23:01:05.167627Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: makeTimestamp("0001-01-01T00:00:00Z"),
              total_rows_written: new Long(0),
            },
          },
          index_name: "INDEX",
          index_type: "secondary",
          create_statement:
            "CREATE INDEX jobs_created_by_type_created_by_id_idx ON system.public.jobs USING btree (created_by_type ASC, created_by_id ASC) STORING (status)",
        },
      ],
      last_reset: makeTimestamp("2021-11-12T20:18:22.167627Z"),
    });

    await driver.refreshIndexStats();

    driver.assertProperties({
      databaseName: "DATABASE",
      tableName: "TABLE",
      indexName: "INDEX",
      details: {
        loading: false,
        loaded: true,
        createStatement:
          "CREATE INDEX jobs_created_by_type_created_by_id_idx ON system.public.jobs USING btree (created_by_type ASC, created_by_id ASC) STORING (status)",
        totalReads: 2,
        lastRead: util.TimestampToMoment(
          makeTimestamp("2021-11-19T23:01:05.167627Z"),
        ),
        lastReset: util.TimestampToMoment(
          makeTimestamp("2021-11-12T20:18:22.167627Z"),
        ),
      },
    });
  });
});
