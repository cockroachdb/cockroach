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
import RangeSelect, {
  RangeOption,
} from "src/views/cluster/components/range/index";
import { mount } from "enzyme";
import moment from "moment";
import { TimeWindow } from "src/redux/timewindow";
import { assert } from "chai";
import "src/enzymeInit";

describe("RangeSelect", function () {
  describe("basic dropdown", function () {
    it("should show all options on dropdown activation", function () {
      const options: RangeOption[] = [
        { value: "1", label: "1", timeLabel: "1" },
        { value: "2", label: "2", timeLabel: "2" },
        { value: "3", label: "3", timeLabel: "3" },
      ];
      const value: TimeWindow = {
        start: moment.utc().subtract(moment.duration(1, "day")),
        end: moment.utc(),
      };
      const rangeSelect = mount(
        <RangeSelect
          options={options}
          onChange={() => {}}
          changeDate={() => {}}
          value={value}
          selected={{}}
          onOpened={() => {}}
          useTimeRange={false}
        />,
      );

      rangeSelect.find(".click-zone").simulate("click");
      assert.lengthOf(rangeSelect.find("button .__option-label"), 3);
    });
  });
});
