// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useScheduleFunction } from "./hooks";
import {
  render,
  cleanup,
  screen,
  fireEvent,
  waitFor,
} from "@testing-library/react";
import moment from "moment-timezone";

describe("useScheduleFunction", () => {
  let mockFn: jest.Mock;

  beforeEach(() => {
    mockFn = jest.fn();
  });

  afterEach(() => {
    cleanup();
  });

  it("should schedule the function according to the lastCompleted time and interval provided", async () => {
    const timeoutMs = 1500;
    const TestScheduleHook = () => {
      const [lastUpdated, setLastUpdated] = React.useState<moment.Moment>(null);

      const setLastUpdatedToNow = () => {
        mockFn();
        setLastUpdated(moment.utc());
      };

      useScheduleFunction(setLastUpdatedToNow, true, timeoutMs, lastUpdated);
      return <div />;
    };

    const { unmount } = render(<TestScheduleHook />);
    await waitFor(
      () => new Promise(res => setTimeout(res, timeoutMs * 3 + 1000)), // Add 0.5s of buffer.
      { timeout: timeoutMs * 3 + 1500 },
    );
    // 3 intervals and the initial call on mount, since last completed was initially null.
    expect(mockFn).toBeCalledTimes(4);

    unmount();
    // Verify scheduling is stopped on unmount.
    await waitFor(
      () => new Promise(res => setTimeout(res, timeoutMs + 500)), // Add 0.5s of buffer.
      { timeout: timeoutMs + 550 },
    );
    expect(mockFn).toBeCalledTimes(4);
    // });
  }, 20000);

  it("should schedule the function immediately if returned scheduleNow is used", async () => {
    const TestScheduleHook = () => {
      const [lastUpdated, setLastUpdated] = React.useState<moment.Moment>(
        moment.utc(),
      );

      const setLastUpdatedToNow = () => {
        mockFn();
        setLastUpdated(moment.utc());
      };

      const [scheduleNow] = useScheduleFunction(
        setLastUpdatedToNow,
        true,
        3000, // Longer timeout.
        lastUpdated,
      );

      const onClick = () => {
        scheduleNow();
      };

      return (
        <div>
          <button onClick={onClick}></button>
        </div>
      );
    };

    render(<TestScheduleHook />);
    const button = await screen.getByRole("button");

    fireEvent.click(button);
    await waitFor(() => expect(mockFn).toBeCalledTimes(1), { timeout: 1500 });

    fireEvent.click(button);
    await waitFor(() => expect(mockFn).toBeCalledTimes(2), { timeout: 1500 });

    // Verify that the schedule is set up correctly after by waiting the interval length.
    await waitFor(() => new Promise(res => setTimeout(res, 3100)), {
      timeout: 4000,
    });
    expect(mockFn).toBeCalledTimes(3);
  }, 10000);

  it("should clear the scheduled func immediately if clear is used", async () => {
    const TestScheduleHook = () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const [_, clear] = useScheduleFunction(mockFn, true, 1000, moment.utc());

      React.useEffect(() => {
        clear();
      });

      return <div />;
    };

    render(<TestScheduleHook />);
    await waitFor(() => new Promise(res => setTimeout(res, 5000)), {
      timeout: 5500,
    });
    expect(mockFn).toBeCalledTimes(0);
  }, 10000);

  it("should not reschedule the func if shouldReschedule=false", async () => {
    const TestScheduleHook = () => {
      // Since we pass in a last completed time here, we should expect no calls.
      useScheduleFunction(mockFn, false, 100, moment.utc());
      return <div />;
    };

    render(<TestScheduleHook />);
    await waitFor(() => new Promise(res => setTimeout(res, 3000)), {
      timeout: 3500,
    });
    expect(mockFn).toBeCalledTimes(0);
  });
});
