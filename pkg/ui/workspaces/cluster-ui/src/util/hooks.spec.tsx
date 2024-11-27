// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  render,
  cleanup,
  screen,
  fireEvent,
  waitFor,
} from "@testing-library/react";
import { renderHook } from "@testing-library/react-hooks";
import moment from "moment-timezone";
import React, { ReactNode } from "react";
import * as SwrModule from "swr";
import * as SwrImmutableModule from "swr/immutable";

import { ClusterDetailsContext, ClusterDetailsContextType } from "../contexts";

import {
  useScheduleFunction,
  useSwrImmutableWithClusterId,
  useSwrWithClusterId,
} from "./hooks";

interface Props {
  children?: ReactNode;
}

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

describe("useSwrWithClusterId", () => {
  const clusterId = "myClusterId";
  const keyStr = "myKey";
  const keyObj = { name: keyStr };
  const keyArr = [keyStr];
  let spyUseSWR: jest.SpyInstance;
  let spyUseSWRImmutable: jest.SpyInstance;
  const nullClusterIdContext: ClusterDetailsContextType = {
    isTenant: false,
    clusterId: null,
  };
  const undefinedClusterIdContext: ClusterDetailsContextType = {};
  const clusterIdContext: ClusterDetailsContextType = {
    isTenant: false,
    clusterId,
  };
  beforeEach(() => {
    spyUseSWR = jest.spyOn(SwrModule, "default");
    spyUseSWRImmutable = jest.spyOn(SwrImmutableModule, "default");
  });
  afterEach(() => {
    jest.restoreAllMocks();
  });
  const testCases = [
    // Object based keys
    {
      name: "object key with null cluster context",
      ctx: nullClusterIdContext,
      key: keyObj,
      expectedKey: { clusterId: null, ...keyObj },
    },
    {
      name: "object key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: keyObj,
      expectedKey: { clusterId: undefined, ...keyObj },
    },
    {
      name: "object key with set cluster context",
      ctx: clusterIdContext,
      key: keyObj,
      expectedKey: { clusterId, ...keyObj },
    },

    // Array based keys
    {
      name: "array key with null cluster context",
      ctx: nullClusterIdContext,
      key: keyArr,
      expectedKey: [null, ...keyArr],
    },
    {
      name: "array key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: keyArr,
      expectedKey: [undefined, ...keyArr],
    },
    {
      name: "array key with set cluster context",
      ctx: clusterIdContext,
      key: keyArr,
      expectedKey: [clusterId, ...keyArr],
    },

    // String based keys
    {
      name: "string key with null cluster context",
      ctx: nullClusterIdContext,
      key: keyStr,
      expectedKey: [null, keyStr],
    },
    {
      name: "string key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: keyStr,
      expectedKey: [undefined, keyStr],
    },
    {
      name: "string key with set cluster context",
      ctx: clusterIdContext,
      key: keyStr,
      expectedKey: [clusterId, keyStr],
    },

    // boolean(true) keys
    {
      name: "boolean (true) key with null cluster context",
      ctx: nullClusterIdContext,
      key: true,
      expectedKey: [null, true],
    },
    {
      name: "boolean (true) key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: true,
      expectedKey: [undefined, true],
    },
    {
      name: "boolean (true) key with set cluster context",
      ctx: clusterIdContext,
      key: true,
      expectedKey: [clusterId, true],
    },

    // boolean(false) keys
    {
      name: "boolean(false) key with null cluster context",
      ctx: nullClusterIdContext,
      key: false,
      expectedKey: false,
    },
    {
      name: "boolean(false) key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: false,
      expectedKey: false,
    },
    {
      name: "boolean(false) key with set cluster context",
      ctx: clusterIdContext,
      key: false,
      expectedKey: false,
    },

    {
      name: "null key with null cluster context",
      ctx: nullClusterIdContext,
      key: null,
      expectedKey: null,
    },
    {
      name: "null key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: null,
      expectedKey: null,
    },
    {
      name: "null key with set cluster context",
      ctx: clusterIdContext,
      key: null,
      expectedKey: null,
    },

    {
      name: "undefined key with null cluster context",
      ctx: nullClusterIdContext,
      key: undefined,
      expectedKey: undefined,
    },
    {
      name: "undefined key with undefined cluster context",
      ctx: undefinedClusterIdContext,
      key: undefined,
      expectedKey: undefined,
    },
    {
      name: "undefined key with set cluster context",
      ctx: clusterIdContext,
      key: undefined,
      expectedKey: undefined,
    },
  ];
  test.each(testCases)("$name", ({ ctx, key, expectedKey }) => {
    const wrapper = makeContextProvidedWrapper(ctx);
    renderHook(() => useSwrWithClusterId(key, (): void => null), {
      wrapper,
    });
    expect(spyUseSWR).toHaveBeenCalledWith(
      expectedKey,
      expect.anything(),
      undefined,
    );

    renderHook(() => useSwrImmutableWithClusterId(key, (): void => null), {
      wrapper,
    });
    expect(spyUseSWRImmutable).toHaveBeenCalledWith(
      expectedKey,
      expect.anything(),
      undefined,
    );
  });
});

const makeContextProvidedWrapper = (ctx: ClusterDetailsContextType) => {
  return ({ children }: Props) => {
    return (
      <ClusterDetailsContext.Provider value={ctx}>
        {children}
      </ClusterDetailsContext.Provider>
    );
  };
};
