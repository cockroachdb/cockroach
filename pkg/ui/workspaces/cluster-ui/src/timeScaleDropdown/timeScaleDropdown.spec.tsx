// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { assert } from "chai";
import { mount } from "enzyme";
import moment from "moment-timezone";
import React, { useState } from "react";
import { MemoryRouter } from "react-router";

import { timeFormat as customMenuTimeFormat } from "../dateRangeMenu";

import RangeSelect from "./rangeSelect";
import { TimeFrameControls } from "./timeFrameControls";
import {
  formatRangeSelectSelected,
  generateDisabledArrows,
  timeFormat as dropdownTimeFormat,
  dateFormat as dropdownDateFormat,
  TimeScaleDropdownProps,
  TimeScaleDropdown,
} from "./timeScaleDropdown";
import * as timescale from "./timeScaleTypes";
import { TimeWindow, ArrowDirection, TimeScale } from "./timeScaleTypes";

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

describe("<TimeScaleDropdown> component", function () {
  jest.useFakeTimers("modern");

  // Returns a new moment every time, so that we don't accidentally mutate it.
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
    jest.setSystemTime(getNow().toDate());
  });

  it("updates as different preset options are selected", async () => {
    const mockSetTimeScale = jest.fn();
    const { getByText, queryByText, getByTestId, baseElement } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );

    // Default state.
    getByText("Past Hour");
    getByText("1h");
    expect(queryByText("Past 6 Hours")).toBeNull();

    // Select a different preset option.
    userEvent.click(getByTestId("dropdown-button"));
    // Dropdown menu is attached to <body> element (it is not a child of the component)
    // and needs to be queried within Body element (baseElement).
    const pastSixHoursOption = Array.from(
      baseElement.querySelectorAll<HTMLButtonElement>(
        ".range-selector.__options button",
      ),
    ).find(el => el.textContent.includes("Past 6 Hours"));
    expect(pastSixHoursOption).toBeDefined();
    pastSixHoursOption.click();
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);
    expect(queryByText("Past Hour")).toBeNull();
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
    getByText("Past Hour");

    // Click left, and it shows a custom time.
    userEvent.click(
      getByRole("button", {
        name: "previous time interval",
      }),
    );
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);
    for (const expectedText of getExpectedCustomText(
      getNow().subtract(moment.duration(1, "h")),
      getNow().subtract(moment.duration(1 * 2, "h")),
      "1h",
    )) {
      getByText(expectedText);
    }

    // Click right, and it reverts to "Past Hour".
    userEvent.click(
      getByRole("button", {
        name: "next time interval",
      }),
    );
    userEvent.click(
      getByRole("button", {
        name: "next time interval",
      }),
    );
    expect(mockSetTimeScale).toHaveBeenCalledTimes(3);
    getByText("Past Hour");
  });

  it("initializes the custom selection to the current time interval", () => {
    const mockSetTimeScale = jest.fn();
    // Default state
    const { getByText, getByDisplayValue } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );
    // Switch to a bigger time interval
    userEvent.click(getByText("Past Hour"));
    userEvent.click(getByText("Past 6 Hours"));
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);

    // Open the custom menu
    userEvent.click(getByText("Past 6 Hours"));
    userEvent.click(getByText("Custom time interval"));
    expect(mockSetTimeScale).toHaveBeenCalledTimes(1);

    // Custom menu should be initialized to currently selected time, i.e. now-6h to now.
    // Start: 3:28 AM (UTC)
    // End: 9:28 AM (UTC)
    const startText = getNow().subtract(6, "h").format(customMenuTimeFormat);
    const endMoment = getNow();
    getByDisplayValue(startText); // start
    getByDisplayValue(endMoment.format(customMenuTimeFormat)); // end

    // Testing changing and selecting a new custom time is blocked on being able to distinguish the time options in the
    // start and end dropdowns; for an attempt see: https://github.com/jocrl/cockroach/commit/a15ac08b3ed0515a4c4910396e32dc8712cc86ec#diff-491a1b9fd6a93863973c270c8c05ab0d28e0a41f616ecd2222df9fab327806f2R196.
  });

  it("opens directly to the custom menu when a custom time interval is currently selected", async () => {
    const mockSetTimeScale = jest.fn();
    const { getByText, getByRole, baseElement } = render(
      <MemoryRouter>
        <TimeScaleDropdownWrapper
          currentScale={new timescale.TimeScaleState().scale}
          onSetTimeScale={mockSetTimeScale}
        />
      </MemoryRouter>,
    );

    // When a preset option is selected, the dropdown should open to other preset options.
    userEvent.click(getByText("Past Hour"));
    getByText("Past 30 Minutes");
    getByText("Past 6 Hours");

    // Change to a custom selection
    userEvent.click(
      getByRole("button", {
        name: "previous time interval",
      }),
    );

    // When a custom option is selected, the dropdown should open to the custom selector.
    const expectedText = getExpectedCustomText(
      getNow().subtract(moment.duration(1, "h")),
      getNow().subtract(moment.duration(1 * 2, "h")),
      "1h",
    );
    getByText(expectedText[0]);
    const customTimeIntervalMenu = baseElement.querySelector<HTMLDivElement>(
      ".range-selector.__custom",
    );
    expect(
      customTimeIntervalMenu.textContent.includes("Start (UTC)"),
    ).toBeTruthy();
    expect(
      customTimeIntervalMenu.textContent.includes("End (UTC)"),
    ).toBeTruthy();

    const presetButtons = Array.from(
      baseElement.querySelectorAll<HTMLButtonElement>(
        ".range-selector.__options button",
      ),
    );
    const backTimeIntervalsButton = Array.from(
      baseElement.querySelectorAll<HTMLButtonElement>(
        ".range-selector.__custom span",
      ),
    ).find(el => el.textContent.includes("Preset time intervals"));
    expect(backTimeIntervalsButton).toBeDefined();
    // Clicking "Preset time intervals" should bring the dropdown back to the preset options.
    backTimeIntervalsButton.click();

    expect(
      presetButtons.find(el => el.textContent.includes("Past 30 Minutes")),
    ).toBeUndefined();
    expect(
      presetButtons.find(el => el.textContent.includes("Past 6 Hours")),
    ).toBeUndefined();
  });
});

const initialEntries = [
  "#/metrics/overview/cluster", // Past Hour
  `#/metrics/overview/cluster/cluster?start=${moment()
    .subtract(1, "hour")
    .format("X")}&end=${moment().format("X")}`, // Past hour
  `#/metrics/overview/cluster/cluster?start=${moment()
    .subtract(6, "hours")
    .format("X")}&end=${moment().format("X")}`, // Past 6 hours
  "#/metrics/overview/cluster/cluster?start=1584528492&end=1584529092", // 10 minutes
  "#/metrics/overview/cluster?start=1583319565&end=1584529165", // 2 weeks
  "#/metrics/overview/node/1", // Node 1 - Past Hour
  `#/metrics/overview/node/2?start=${moment()
    .subtract(10, "minutes")
    .format("X")}&end=${moment().format("X")}`, // Node 2 - Past Hour
  "#/metrics/overview/node/3?start=1584528726&end=1584529326", // Node 3 - 10 minutes
];

describe("TimeScaleDropdown functions", function () {
  jest.useFakeTimers("modern");
  let state: Omit<TimeScaleDropdownProps, "setTimeScale">;
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
    jest.setSystemTime(new Date(2020, 5, 1, 9, 28, 30));
    const timeScaleState = new timescale.TimeScaleState();
    setCurrentWindowFromTimeScale(timeScaleState.scale);
    state = {
      currentScale: timeScaleState.scale,
      // setTimeScale: () => {},
    };
  });

  it("valid path should not redirect to 404", () => {
    const wrapper = makeTimeScaleDropdown(state);
    assert.equal(wrapper.find(RangeSelect).length, 1);
    assert.equal(wrapper.find(TimeFrameControls).length, 1);
  });

  describe("formatRangeSelectSelected", () => {
    it("formatRangeSelectSelected must return title Past Hour", () => {
      const title = formatRangeSelectSelected(
        currentWindow,
        state.currentScale,
        "UTC",
      );
      assert.deepEqual(title, {
        key: "Past Hour",
        timeLabel: "1h",
        timeWindow: currentWindow,
      });
    });

    it("returns custom Title with Time part only for current day", () => {
      const currentScale = { ...state.currentScale, key: "Custom" };
      const title = formatRangeSelectSelected(
        currentWindow,
        currentScale,
        "UTC",
      );
      const timeStart = moment
        .utc(currentWindow.start)
        .format(dropdownTimeFormat);
      const timeEnd = moment.utc(currentWindow.end).format(dropdownTimeFormat);
      const wrapper = makeTimeScaleDropdown({ ...state, currentScale });
      assert.equal(
        wrapper.find(".trigger .Select-value-label").first().text(),
        ` ${timeStart} -  ${timeEnd} (UTC)`,
      );
      assert.deepEqual(title, {
        dateStart: "",
        dateEnd: "",
        timeStart,
        timeEnd,
        key: "Custom",
        timeLabel: "1h",
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
      const title = formatRangeSelectSelected(window, currentScale, "UTC");
      const timeStart = moment.utc(window.start).format(dropdownTimeFormat);
      const timeEnd = moment.utc(window.end).format(dropdownTimeFormat);
      const dateStart = moment.utc(window.start).format(dropdownDateFormat);
      const dateEnd = moment.utc(window.end).format(dropdownDateFormat);
      const wrapper = makeTimeScaleDropdown({
        ...state,
        currentScale,
      });
      assert.equal(
        wrapper.find(".trigger .Select-value-label").first().text(),
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
    expect(arrows).toEqual([ArrowDirection.CENTER, ArrowDirection.RIGHT]);
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
