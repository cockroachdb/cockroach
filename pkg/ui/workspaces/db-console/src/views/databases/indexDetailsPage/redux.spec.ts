// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  IndexDetailPageActions,
  IndexDetailsPageData,
  util,
  TimeScale,
} from "@cockroachlabs/cluster-ui";
import { createMemoryHistory } from "history";
import Long from "long";
import moment from "moment-timezone";
import { RouteComponentProps } from "react-router-dom";
import { bindActionCreators, Store } from "redux";

import { AdminUIState, createAdminUIStore } from "src/redux/state";
import {
  databaseNameAttr,
  indexNameAttr,
  tableNameAttr,
} from "src/util/constants";
import * as fakeApi from "src/util/fakeApi";

import { mapStateToProps, mapDispatchToProps } from "./redux";

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

const timeScale: TimeScale = {
  key: "Past 10 Minutes",
  windowSize: moment.duration(10, "minutes"),
  windowValid: moment.duration(10, "seconds"),
  sampleSize: moment.duration(10, "seconds"),
  fixedWindowEnd: false,
};

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

  assertProperties(expected: IndexDetailsPageData, compareTimestamps = true) {
    // Assert moments are equal if not in pre-loading state.
    if (compareTimestamps) {
      expect(
        this.properties().details.lastRead.isSame(expected.details.lastRead),
      ).toBe(true);
      expect(
        this.properties().details.lastReset.isSame(expected.details.lastReset),
      ).toBe(true);
    }
    // Assert objects without moments are equal.
    const props = this.properties();
    delete props.details.lastRead;
    delete expected.details.lastRead;
    delete props.details.lastReset;
    delete expected.details.lastReset;
    delete props.timeScale;
    delete expected.timeScale;
    expect(props).toEqual(expected);
  }

  async refreshIndexStats() {
    return this.actions.refreshIndexStats(this.database, this.table);
  }
}

describe("Index Details Page", function () {
  let driver: TestDriver;

  beforeEach(function () {
    driver = new TestDriver(
      createAdminUIStore(createMemoryHistory()),
      "DATABASE",
      "TABLE",
      "INDEX",
    );
  });

  afterEach(function () {
    fakeApi.restore();
  });

  it("starts in a pre-loading state", function () {
    driver.assertProperties(
      {
        databaseName: "DATABASE",
        tableName: "TABLE",
        indexName: "INDEX",
        isTenant: false,
        nodeRegions: {},
        hasAdminRole: undefined,
        hasViewActivityRedactedRole: undefined,
        timeScale: timeScale,
        details: {
          loading: false,
          loaded: false,
          createStatement: "",
          totalReads: 0,
          indexRecommendations: [],
          tableID: undefined,
          indexID: undefined,
          lastRead: util.minDate,
          lastReset: util.minDate,
          databaseID: undefined,
        },
      },
      false,
    );
  });

  it("loads index stats", async function () {
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
              last_read: util.stringToTimestamp("2021-11-19T23:01:05.167627Z"),
              total_rows_read: new Long(0),
              total_write_count: new Long(0),
              last_write: util.stringToTimestamp("0001-01-01T00:00:00Z"),
              total_rows_written: new Long(0),
            },
          },
          index_name: "INDEX",
          index_type: "secondary",
          create_statement:
            "CREATE INDEX jobs_created_by_type_created_by_id_idx ON system.public.jobs USING btree (created_by_type ASC, created_by_id ASC) STORING (status)",
        },
      ],
      last_reset: util.stringToTimestamp("2021-11-12T20:18:22.167627Z"),
      database_id: 10,
    });

    await driver.refreshIndexStats();

    driver.assertProperties({
      databaseName: "DATABASE",
      tableName: "TABLE",
      indexName: "INDEX",
      isTenant: false,
      nodeRegions: {},
      timeScale: timeScale,
      hasAdminRole: undefined,
      hasViewActivityRedactedRole: undefined,
      details: {
        loading: false,
        loaded: true,
        tableID: "15",
        indexID: "2",
        createStatement:
          "CREATE INDEX jobs_created_by_type_created_by_id_idx ON system.public.jobs USING btree (created_by_type ASC, created_by_id ASC) STORING (status)",
        totalReads: 2,
        lastRead: util.TimestampToMoment(
          util.stringToTimestamp("2021-11-19T23:01:05.167627Z"),
        ),
        lastReset: util.TimestampToMoment(
          util.stringToTimestamp("2021-11-12T20:18:22.167627Z"),
        ),
        indexRecommendations: [],
        databaseID: 10,
      },
    });
  });
});
