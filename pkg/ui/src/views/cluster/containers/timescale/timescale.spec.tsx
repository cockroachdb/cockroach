// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
// import { assert } from "chai";
import { mount } from "enzyme";
import _ from "lodash";
import {
  TimeScaleDropdown,
  TimeScaleDropdownProps,
  getTimeRangeTitle,
  generateDisabledArrows,
} from "./index";
import * as timewindow from "src/redux/timewindow";
// import sinon from "sinon";
import moment from "moment";
import { refreshNodes } from "src/redux/apiReducers";
import "src/enzymeInit";
import { MemoryRouter } from "react-router";
import TimeFrameControls from "../../components/controls";
import RangeSelect from "../../components/range";
import { expect, assert } from "chai";
import sinon from "sinon";
import { ArrowDirection } from "oss/src/views/shared/components/dropdown";

const initialEntries = [
  "#/metrics/overview/cluster", // Past 10 minutes
  `#/metrics/overview/cluster/cluster?start=${moment()
    .subtract(1, "hour")
    .format("X")}&end=${moment().format("X")}`, // Past 1 hour
  `#/metrics/overview/cluster/cluster?start=${moment()
    .subtract(6, "hours")
    .format("X")}&end=${moment().format("X")}`, // Past 6 hours
  "#/metrics/overview/cluster/cluster?start=1584528492&end=1584529092", // 10 minutes
  "#/metrics/overview/cluster?start=1583319565&end=1584529165", // 2 weeks
  "#/metrics/overview/node/1", // Node 1 - Past 10 minutes
  `#/metrics/overview/node/2?start=${moment()
    .subtract(10, "minutes")
    .format("X")}&end=${moment().format("X")}`, // Node 2 - Past 10 minutes
  "#/metrics/overview/node/3?start=1584528726&end=1584529326", // Node 3 - 10 minutes
];

const dateFormat = "MMM DD,";
const timeFormat = "h:mmA";

describe("<TimeScaleDropdown>", function () {
  let state: TimeScaleDropdownProps;
  let spy: sinon.SinonSpy;

  const makeTimeScaleDropdown = (props: TimeScaleDropdownProps) =>
    mount(
      <MemoryRouter initialEntries={initialEntries}>
        <TimeScaleDropdown {...props} />
      </MemoryRouter>,
    );

  beforeEach(function () {
    const timewindowState = new timewindow.TimeWindowState();
    state = {
      currentScale: timewindowState.scale,
      currentWindow: { start: moment().subtract(10, "minutes"), end: moment() },
      nodeStatusesValid: false,
      useTimeRange: timewindowState.useTimeRange,
      refreshNodes,
      setTimeScale: timewindow.setTimeScale,
      setTimeRange: timewindow.setTimeRange,
    };
    spy = sinon.spy();
  });

  it("refreshes nodes when mounted.", () => {
    makeTimeScaleDropdown({ ...state, refreshNodes: spy });
    assert.isTrue(spy.called);
  });

  it("valid path should not redirect to 404", () => {
    const wrapper = makeTimeScaleDropdown(state);
    expect(wrapper.find(RangeSelect)).to.have.lengthOf(1);
    expect(wrapper.find(TimeFrameControls)).to.have.lengthOf(1);
  });

  it("Past 10 minutes must be render", () => {
    const wrapper = makeTimeScaleDropdown(state);
    wrapper.setProps({ currentScale: state.currentScale });
    assert.equal(
      wrapper.props().currentScale,
      timewindow.availableTimeScales["Past 10 Minutes"],
    );
  });

  it("getTimeRangeTitle must return title Past 10 Minutes", () => {
    const title = getTimeRangeTitle(state.currentWindow, state.currentScale);
    const wrapper = makeTimeScaleDropdown(state);
    console.log(wrapper.find(".trigger .Select-value-label"));
    expect(
      wrapper.find(".trigger .Select-value-label").first().text(),
    ).to.be.eql(`Past 10 Minutes`);
    assert.deepEqual(title, { title: "Past 10 Minutes" });
  });

  it("getTimeRangeTitle must return custom Title", () => {
    const currentScale = { ...state.currentScale, key: "Custom" };
    const title = getTimeRangeTitle(state.currentWindow, currentScale);
    const timeStart = moment.utc(state.currentWindow.start).format(timeFormat);
    const timeEnd = moment.utc(state.currentWindow.end).format(timeFormat);
    const wrapper = makeTimeScaleDropdown({ ...state, currentScale });
    expect(
      wrapper.find(".trigger .Select-value-label").first().text(),
    ).to.be.eql(` ${timeStart} -  ${timeEnd}`);
    assert.deepEqual(title, {
      dateStart: "",
      dateEnd: "",
      timeStart,
      timeEnd,
      title: "Custom",
    });
  });

  it("getTimeRangeTitle must return custom Title", () => {
    const currentWindow = {
      start: moment(state.currentWindow.start).subtract(1, "day"),
      end: moment(state.currentWindow.end).subtract(1, "day"),
    };
    const currentScale = { ...state.currentScale, key: "Custom" };
    const title = getTimeRangeTitle(currentWindow, currentScale);
    const timeStart = moment.utc(currentWindow.start).format(timeFormat);
    const timeEnd = moment.utc(currentWindow.end).format(timeFormat);
    const dateStart = moment.utc(currentWindow.start).format(dateFormat);
    const dateEnd = moment.utc(currentWindow.end).format(dateFormat);
    const wrapper = makeTimeScaleDropdown({
      ...state,
      currentWindow,
      currentScale,
    });
    expect(
      wrapper.find(".trigger .Select-value-label").first().text(),
    ).to.be.eql(`${dateStart} ${timeStart} - ${dateEnd} ${timeEnd}`);
    assert.deepEqual(title, {
      dateStart,
      dateEnd,
      timeStart,
      timeEnd,
      title: "Custom",
    });
  });

  it("generateDisabledArrows must return array with disabled buttons", () => {
    const arrows = generateDisabledArrows(state.currentWindow);
    const wrapper = makeTimeScaleDropdown(state);
    expect(
      wrapper.find(".controls-content ._action.disabled"),
    ).to.have.lengthOf(2);
    assert.deepEqual(arrows, [ArrowDirection.CENTER, ArrowDirection.RIGHT]);
  });

  it("generateDisabledArrows must render 3 active buttons and return empty array", () => {
    const currentWindow = {
      start: moment(state.currentWindow.start).subtract(1, "day"),
      end: moment(state.currentWindow.end).subtract(1, "day"),
    };
    const arrows = generateDisabledArrows(currentWindow);
    const wrapper = makeTimeScaleDropdown({ ...state, currentWindow });
    expect(
      wrapper.find(".controls-content ._action.disabled"),
    ).to.have.lengthOf(0);
    assert.deepEqual(arrows, []);
  });
});
