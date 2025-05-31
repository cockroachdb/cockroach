import React from "react";
import moment, { Moment } from "moment-timezone";
import { render, screen, fireEvent, within, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom/extend-expect";

import RangeSelect, { RangeOption, Selected } from "./rangeSelect";
import { TimeWindow } from "./timeScaleTypes";
import { TimezoneContext } from "../contexts"; // Assuming path to TimezoneContext

// Mock DateRangeMenu
jest.mock("src/dateRangeMenu", () => ({
  DateRangeMenu: jest.fn((props: any) => (
    <div
      data-testid="mock-date-range-menu"
      data-startinit={props.startInit?.toISOString()}
      data-endinit={props.endInit?.toISOString()}
    />
  )),
}));

// Mock CaretDown icon
jest.mock("src/icon/caretDown", () => ({
  CaretDown: () => <span data-testid="caret-down" />,
}));

// Mock Text and Timezone from src if they are complex or cause issues
jest.mock("src/text", () => ({
  ...jest.requireActual("src/text"), // Keep actual implementation for TextTypes if needed
  Text: jest.fn(({ children, className }) => <div className={className}>{children}</div>),
}));
jest.mock("src/timestamp", () => ({
  Timezone: () => <span data-testid="mock-timezone-display" />,
}));


const mockOnPresetOptionSelect = jest.fn();
const mockOnCustomSelect = jest.fn();

const defaultStandardOptions: RangeOption[] = [
  { value: "Past 1 Hour", label: "Past 1 Hour", timeLabel: "1h" },
  { value: "Past 6 Hours", label: "Past 6 Hours", timeLabel: "6h" },
  { value: "Custom", label: "Custom", timeLabel: "--" },
];

const defaultSelectedProps: Selected = {
  key: "Past 1 Hour",
  timeLabel: "1h",
  timeWindow: {
    start: moment().subtract(1, "hour"),
    end: moment(),
  },
};

const testTimezone = "America/New_York";

const renderRangeSelect = (propsOverride: Partial<React.ComponentProps<typeof RangeSelect>> = {}) => {
  const props = {
    options: defaultStandardOptions,
    onPresetOptionSelect: mockOnPresetOptionSelect,
    onCustomSelect: mockOnCustomSelect,
    selected: defaultSelectedProps,
    timezone: testTimezone, // Pass timezone prop
    recentCustomIntervals: [], // Default to empty
    ...propsOverride,
  };
  return render(
    <TimezoneContext.Provider value={testTimezone}>
      <RangeSelect {...props} />
    </TimezoneContext.Provider>,
  );
};

// Helper to open the dropdown
const openDropdown = () => {
  fireEvent.click(screen.getByTestId("dropdown-button"));
};

describe("<RangeSelect />", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("Displaying Recent Custom Intervals", () => {
    it("does not render 'Recently Used' section if recentCustomIntervals is empty", () => {
      renderRangeSelect({ recentCustomIntervals: [] });
      openDropdown();
      expect(screen.queryByText("Recently Used")).toBeNull();
      expect(screen.queryByText(/Recent:/)).toBeNull(); // Check for any button with "Recent:"
    });

    it("does not render 'Recently Used' section if recentCustomIntervals is undefined", () => {
      renderRangeSelect({ recentCustomIntervals: undefined });
      openDropdown();
      expect(screen.queryByText("Recently Used")).toBeNull();
    });

    describe("With Recent Intervals", () => {
      const recentIntervals: TimeWindow[] = [
        { start: moment.utc("2023-03-10T10:00:00Z"), end: moment.utc("2023-03-10T14:00:00Z") }, // 4h duration
        { start: moment.utc("2023-03-09T08:30:00Z"), end: moment.utc("2023-03-09T09:00:00Z") }, // 30m duration
      ];

      beforeEach(() => {
        renderRangeSelect({ recentCustomIntervals: recentIntervals });
        openDropdown();
      });

      it("renders 'Recently Used' title and separator", () => {
        expect(screen.getByText("Recently Used")).toBeInTheDocument();
        // Check for separator class (implementation detail, but useful for structure)
        // This relies on the class name `recent-intervals-separator` being present.
        const separator = document.querySelector(".recent-intervals-separator");
        expect(separator).toBeInTheDocument();
      });

      it("renders an OptionButton for each recent interval", () => {
        // Standard options + recent options
        // Buttons are identified by their structure (TimeLabel + option label)
        // We look for the specific labels of recent items.
        expect(screen.getByText(/Mar 10, 06:00 - 10:00/)).toBeInTheDocument(); // Formatted to testTimezone (UTC-4 for EDT)
        expect(screen.getByText(/Mar 9, 04:30 - 05:00/)).toBeInTheDocument();  // Formatted to testTimezone
      });

      it("formats labels correctly for recent intervals (including timezone)", () => {
        // Example: 2023-03-10T10:00:00Z is 6:00 AM in America/New_York (EDT is UTC-4)
        // Example: 2023-03-10T14:00:00Z is 10:00 AM in America/New_York
        const firstIntervalButton = screen.getByText(/Mar 10, 06:00 - 10:00/);
        expect(firstIntervalButton).toBeInTheDocument();
        
        // Check its associated timeLabel (duration)
        const firstIntervalParentButton = firstIntervalButton.closest("button");
        expect(within(firstIntervalParentButton).getByText("4h")).toBeInTheDocument();

        const secondIntervalButton = screen.getByText(/Mar 9, 04:30 - 05:00/);
        expect(secondIntervalButton).toBeInTheDocument();
        const secondIntervalParentButton = secondIntervalButton.closest("button");
        expect(within(secondIntervalParentButton).getByText("30m")).toBeInTheDocument();
      });
    });
  });

  describe("Interacting with Recent Custom Intervals", () => {
    const recentIntervals: TimeWindow[] = [
      { start: moment.utc("2023-03-25T09:00:00Z"), end: moment.utc("2023-03-25T11:00:00Z") },
    ];

    beforeEach(() => {
      renderRangeSelect({ recentCustomIntervals: recentIntervals });
      openDropdown();
    });

    it("calls onCustomSelect with correct moments when a recent interval is clicked", () => {
      // EDT is UTC-4. 09:00Z is 05:00 EDT. 11:00Z is 07:00 EDT.
      const recentButton = screen.getByText(/Mar 25, 05:00 - 07:00/);
      fireEvent.click(recentButton.closest("button"));

      expect(mockOnCustomSelect).toHaveBeenCalledTimes(1);
      const [startArg, endArg] = mockOnCustomSelect.mock.calls[0][0];
      expect(startArg.isSame(recentIntervals[0].start)).toBe(true);
      expect(endArg.isSame(recentIntervals[0].end)).toBe(true);
    });

    it("closes the dropdown after selecting a recent interval", async () => {
      const recentButton = screen.getByText(/Mar 25, 05:00 - 07:00/);
      fireEvent.click(recentButton.closest("button"));
      
      // The dropdown content should disappear. Check for absence of a known element from the open dropdown.
      // For example, the "Recently Used" title or one of the standard options.
      await waitFor(() => {
        expect(screen.queryByText("Recently Used")).not.toBeInTheDocument();
        expect(screen.queryByText("Past 1 Hour")).not.toBeInTheDocument();
      });
    });

    it("highlights a recent interval button if it's the selected one", () => {
      const selectedRecent: Selected = {
        key: "Custom",
        timeWindow: recentIntervals[0],
        timeLabel: "2h", // Calculated for the selected interval
        // dateStart, dateEnd, timeStart, timeEnd would be formatted based on recentIntervals[0] and timezone
        dateStart: "Mar 25,",
        timeStart: "05:00",
        dateEnd: "", // Same day
        timeEnd: "07:00",
      };
      // Re-render with the specific selected prop
      renderRangeSelect({ recentCustomIntervals: recentIntervals, selected: selectedRecent });
      openDropdown();
      
      const recentButtonLabel = screen.getByText(/Mar 25, 05:00 - 07:00/);
      const buttonElement = recentButtonLabel.closest("button");
      expect(buttonElement).toHaveClass("active");
    });
  });

  describe("Interaction with Standard Options", () => {
    const recentIntervals: TimeWindow[] = [
      { start: moment.utc("2023-03-25T09:00:00Z"), end: moment.utc("2023-03-25T11:00:00Z") },
    ];

    beforeEach(() => {
      renderRangeSelect({ recentCustomIntervals: recentIntervals });
      openDropdown();
    });

    it("still renders standard options correctly", () => {
      expect(screen.getByText("Past 1 Hour")).toBeInTheDocument();
      expect(screen.getByText("Past 6 Hours")).toBeInTheDocument();
      expect(screen.getByText("Custom time interval")).toBeInTheDocument(); // "Custom" option's display text
    });

    it("calls onPresetOptionSelect when a standard preset option is clicked", () => {
      const presetButton = screen.getByText("Past 6 Hours").closest("button");
      fireEvent.click(presetButton);
      expect(mockOnPresetOptionSelect).toHaveBeenCalledTimes(1);
      expect(mockOnPresetOptionSelect).toHaveBeenCalledWith(defaultStandardOptions[1]); // "Past 6 Hours"
    });

    it("shows DateRangeMenu mock when 'Custom time interval' is clicked", () => {
      const customButton = screen.getByText("Custom time interval").closest("button");
      fireEvent.click(customButton);
      expect(screen.getByTestId("mock-date-range-menu")).toBeInTheDocument();
    });

    it("correctly highlights a standard option if it's selected", () => {
      // Default props already have "Past 1 Hour" selected
      const presetButton = screen.getByText("Past 1 Hour").closest("button");
      expect(presetButton).toHaveClass("active");
    });
  });
});
