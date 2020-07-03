// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { shallow, mount } from "enzyme";
import { noop } from "lodash";
import { assert } from "chai";
import Long from "long";
import classNames from "classnames/bind";

import "src/enzymeInit";
import { NonTableSummary } from "./nonTableSummary";
import { refreshNonTableStats } from "src/redux/apiReducers";
import { cockroach } from "src/js/protos";
import Loading from "src/views/shared/components/loading";
import NonTableStatsResponse = cockroach.server.serverpb.NonTableStatsResponse;
import { InlineAlert } from "src/components/inlineAlert/inlineAlert";
import styles from "src/components/inlineAlert/inlineAlert.module.styl";

const cn = classNames.bind(styles);

describe("NonTableSummary", () => {
  describe("Loading data", () => {
    it("successfully loads data", () => {
      const tableStatsData = new NonTableStatsResponse({
        internal_use_stats: {
          approximate_disk_bytes: Long.fromNumber(1),
          missing_nodes: [],
          node_count: Long.fromNumber(1),
          range_count: Long.fromNumber(1),
          replica_count: Long.fromNumber(1),
          stats: null,
        },
        time_series_stats: {
          approximate_disk_bytes: Long.fromNumber(1),
          missing_nodes: [],
          node_count: Long.fromNumber(1),
          range_count: Long.fromNumber(1),
          replica_count: Long.fromNumber(1),
          stats: null,
        },
      });
      const wrapper = shallow(<NonTableSummary
        nonTableStats={tableStatsData}
        nonTableStatsValid={true}
        refreshNonTableStats={noop as typeof refreshNonTableStats}
        lastError={undefined} />);
      const loadingWrapper = wrapper.find(Loading).dive();
      assert.isTrue(loadingWrapper.find(".database-summary-table").exists());
    });

    it("shows error message when failed request", () => {
      const error = {
        name: "Forbidden",
        message: "Insufficient privileges to view this resource",
      };

      const expectedMessage = `${error.message}: no details available`;
      const wrapper = mount(<NonTableSummary
        nonTableStats={null}
        nonTableStatsValid={true}
        refreshNonTableStats={noop as typeof refreshNonTableStats}
        lastError={error} />);

      const loadingWrapper = wrapper.find(Loading);
      assert.isTrue(loadingWrapper.exists());

      const alertWrapper = loadingWrapper.find(InlineAlert).at(0);
      assert.equal(alertWrapper.find(`.${cn("title")}`).text(), expectedMessage);
    });
  });
});
