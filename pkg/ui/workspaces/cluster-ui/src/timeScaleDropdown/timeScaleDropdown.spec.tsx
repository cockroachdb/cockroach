// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { mount } from "enzyme";
import {
  formatRangeSelectSelected,
  generateDisabledArrows,
  timeFormat as dropdownTimeFormat,
  dateFormat as dropdownDateFormat,
  TimeScaleDropdownProps,
  TimeScaleDropdown,
} from "./timeScaleDropdown";
import * as timescale from "./timeScaleTypes";
import moment from "moment";
import { MemoryRouter } from "react-router";
import TimeFrameControls from "./timeFrameControls";
import RangeSelect from "./rangeSelect";
import { timeFormat as customMenuTimeFormat } from "../dateRange";
import { assert } from "chai";
import sinon from "sinon";
import { TimeWindow, ArrowDirection, TimeScale } from "./timeScaleTypes";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

/**
 * This wrapper holds the time scale state to allow tests to render a stateful, functional component,
 * while providing onSetTimeScale to listen for this.
 */
function TimeScaleDropdownWrapper({
  currentScale,
  onSetTimeScale,
  ...props
}: Omit<TimeScaleDropdownProps, "setTimeScale"> & {
  onSetTimeScale?: (tw: TimeScale) => void;
}): React.ReactElement {
  const [innerTimeScale, setInnerTimeScale] = useState(currentScale);
  const setTimeScaleWrapper = (tw: timescale.TimeScale) => {
    setInnerTimeScale(tw);
    onSetTimeScale(tw);
  };
  return (
    <TimeScaleDropdown
      currentScale={innerTimeScale}
      setTimeScale={setTimeScaleWrapper}
      {...props}
    />
  );
}

describe("<TimeScaleDropdown> component", function() {
  let clock: sinon.SinonFakeTimers;

  // Returns a new moment every time, so that we don't accidentally mutate it
  const getNow = () => {
    return moment.utc({
      year: 2020,
      month: 5,
      day: 1,
      hour: 9,
      minute: 28,
      second: 30,
    });
  };

  const getExpectedCustomText = (
    startMoment: moment.Moment,
    endMoment: moment.Moment,
    timeLabel?: string,
  ) => {
    const expectedText = [
      startMoment.format(dropdownTimeFormat),
      endMoment.format(dropdownTimeFormat),
      "(UTC)",
    ];
    if (timeLabel) {
      expectedText.push(timeLabel);
    }
    return expectedText;
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers(getNow().toDate());
  });

  afterEach(() => {
    clock.restore();
  });

  it("updates as different preset options are selected", () => {
    const mockSetTimeScale = jest.fn();
    const { getByText, queryByText } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );

    // Default state
    getByText("Past 10 Minutes");
    getByText("10m");
    expect(queryByText("Past 6 Hours")).toBeNull();

    // Select a different preset option
    userEvent.click(getByText("Past 10 Minutes"));
    userEvent.click(getByText("Past 6 Hours"));

    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);
    expect(queryByText("Past 10 Minutes")).toBeNull();
    getByText("Past 6 Hours");
    getByText("6h");
  });

  it("is controlled by next and previous arrow buttons", () => {
    const mockSetTimeScale = jest.fn();
    // Default state
    const { getByText, getByRole } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );
    getByText("Past 10 Minutes");

    // Click left, and it shows a custom time
    userEvent.click(
      getByRole("button", {
        name: "previous timeframe",
      }),
    );
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);
    for (const expectedText of getExpectedCustomText(
      getNow().subtract(moment.duration(10, "m")),
      getNow().subtract(moment.duration(10 * 2, "m")),
      "10m",
    )) {
      getByText(expectedText);
    }

    // Click right, and it reverts to "Past 10 minutes"
    userEvent.click(
      getByRole("button", {
        name: "next timeframe",
      }),
    );
    expect(mockSetTimeScale).toHaveBeenCalledTimes(2);
    getByText("Past 10 Minutes");
  });

  it.only("allows selection of a custom time frame", () => {
    const mockSetTimeScale = jest.fn();
    // Default state
    const { getByText, getByDisplayValue, container } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );
    // Switch to a bigger time frame
    userEvent.click(getByText("Past 10 Minutes"));
    userEvent.click(getByText("Past 6 Hours"));
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);

    // Open the custom menu
    userEvent.click(getByText("Past 6 Hours"));
    userEvent.click(getByText("Custom date range"));
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);

    // Custom menu should be initialized to currently selected time, i.e. now-6h to now
    // Start: 3:28 AM (UTC)
    // End: 9:28 AM (UTC)
    const startText = getNow()
      .subtract(6, "h")
      .format(customMenuTimeFormat);
    const endMoment = getNow();
    getByDisplayValue(startText); // start
    getByDisplayValue(endMoment.format(customMenuTimeFormat)); // end
  });
});

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

describe("TimeScaleDropdown functions", function() {
  let state: Omit<TimeScaleDropdownProps, "setTimeScale">;
  let clock: sinon.SinonFakeTimers;
  let currentWindow: TimeWindow;

  const setCurrentWindowFromTimeScale = (timeScale: TimeScale): void => {
    const end = timeScale.fixedWindowEnd || moment.utc();
    currentWindow = {
      start: moment(end).subtract(timeScale.windowSize),
      end,
    };
  };

  const makeTimeScaleDropdown = (
    props: Omit<TimeScaleDropdownProps, "setTimeScale">,
  ) => {
    setCurrentWindowFromTimeScale(props.currentScale);
    return mount(
      <MemoryRouter initialEntries={initialEntries}>
        <TimeScaleDropdownWrapper {...props} />
      </MemoryRouter>,
    );
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers(new Date(2020, 5, 1, 9, 28, 30));
    const timeScaleState = new timescale.TimeScaleState();
    setCurrentWindowFromTimeScale(timeScaleState.scale);
    state = {
      currentScale: timeScaleState.scale,
      // setTimeScale: () => {},
    };
  });

  afterEach(() => {
    clock.restore();
  });

  it("valid path should not redirect to 404", () => {
    const wrapper = makeTimeScaleDropdown(state);
    assert.equal(wrapper.find(RangeSelect).length, 1);
    assert.equal(wrapper.find(TimeFrameControls).length, 1);
  });

  describe("formatRangeSelectSelected", () => {
    it("formatRangeSelectSelected must return title Past 10 Minutes", () => {
      const _ = makeTimeScaleDropdown(state);

      const title = formatRangeSelectSelected(
        currentWindow,
        state.currentScale,
      );
      assert.deepEqual(title, {
        key: "Past 10 Minutes",
        timeLabel: "10m",
        timeWindow: currentWindow,
      });
    });

    it("returns custom Title with Time part only for current day", () => {
      const currentScale = { ...state.currentScale, key: "Custom" };
      const title = formatRangeSelectSelected(currentWindow, currentScale);
      const timeStart = moment
        .utc(currentWindow.start)
        .format(dropdownTimeFormat);
      const timeEnd = moment.utc(currentWindow.end).format(dropdownTimeFormat);
      const wrapper = makeTimeScaleDropdown({ ...state, currentScale });
      assert.equal(
        wrapper
          .find(".trigger .Select-value-label")
          .first()
          .text(),
        ` ${timeStart} -  ${timeEnd} (UTC)`,
      );
      assert.deepEqual(title, {
        dateStart: "",
        dateEnd: "",
        timeStart,
        timeEnd,
        key: "Custom",
        timeLabel: "10m",
        timeWindow: currentWindow,
      });
    });

    it("returns custom Title with Date and Time part for the range with past days", () => {
      const window: TimeWindow = {
        start: moment(currentWindow.start).subtract(2, "day"),
        end: moment(currentWindow.end).subtract(1, "day"),
      };
      const currentScale = {
        ...state.currentScale,
        fixedWindowEnd: window.end,
        windowSize: moment.duration(
          window.end.diff(window.start, "seconds"),
          "seconds",
        ),
        key: "Custom",
      };
      const title = formatRangeSelectSelected(window, currentScale);
      const timeStart = moment.utc(window.start).format(dropdownTimeFormat);
      const timeEnd = moment.utc(window.end).format(dropdownTimeFormat);
      const dateStart = moment.utc(window.start).format(dropdownDateFormat);
      const dateEnd = moment.utc(window.end).format(dropdownDateFormat);
      const wrapper = makeTimeScaleDropdown({
        ...state,
        currentScale,
      });
      assert.equal(
        wrapper
          .find(".trigger .Select-value-label")
          .first()
          .text(),
        `${dateStart} ${timeStart} - ${dateEnd} ${timeEnd} (UTC)`,
      );
      assert.deepEqual(title, {
        dateStart,
        dateEnd,
        timeStart,
        timeEnd,
        key: "Custom",
        timeLabel: "1d",
        timeWindow: currentWindow,
      });
    });
  });

  it("generateDisabledArrows must return array with disabled buttons", () => {
    const arrows = generateDisabledArrows(currentWindow);
    const wrapper = makeTimeScaleDropdown(state);
    assert.equal(wrapper.find(".controls-content ._action.disabled").length, 2);
    assert.deepEqual(arrows, [ArrowDirection.CENTER, ArrowDirection.RIGHT]);
  });

  it("generateDisabledArrows must render 3 active buttons and return empty array", () => {
    const window: TimeWindow = {
      start: moment(currentWindow.start).subtract(1, "day"),
      end: moment(currentWindow.end).subtract(1, "day"),
    };
    const currentTimeScale = {
      ...state.currentScale,
      fixedWindowEnd: window.end,
    };
    const arrows = generateDisabledArrows(window);
    const wrapper = makeTimeScaleDropdown({
      ...state,
      currentScale: currentTimeScale,
    });
    assert.equal(wrapper.find(".controls-content ._action.disabled").length, 0);
    assert.deepEqual(arrows, []);
  });
});
