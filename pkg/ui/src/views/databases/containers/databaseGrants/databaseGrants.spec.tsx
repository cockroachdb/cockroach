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
import _ from "lodash";
import { mount } from "enzyme";
import { assert } from "chai";
import classNames from "classnames/bind";
import { DatabaseSummaryGrants } from "./";
import {
  emptyProps,
  loadingProps,
  fullfilledProps,
} from "./databaseGrants.fixtures";

import styles from "src/views/shared/components/sortabletable/sortabletable.module.styl";
const cx = classNames.bind(styles);

describe("<DatabaseSummaryGrants>", function () {
  it("render loading state ", function () {
    const wrapper = mount(<DatabaseSummaryGrants {...loadingProps} />);
    assert.lengthOf(wrapper.find(`.${cx("table__loading")}`), 1);
  });

  it("render fulfilled state ", function () {
    const wrapper = mount(<DatabaseSummaryGrants {...fullfilledProps} />);
    assert.lengthOf(wrapper.find(`.${cx("sort-table__row--body")}`), 4);
  });

  it("render empty state ", function () {
    const wrapper = mount(<DatabaseSummaryGrants {...emptyProps} />);
    assert.lengthOf(wrapper.find(`.${cx("table__no-results")}`), 1);
  });
});
