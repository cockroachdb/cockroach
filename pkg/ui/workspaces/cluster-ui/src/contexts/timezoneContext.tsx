// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { createContext, useContext } from "react";

export const CoordinatedUniversalTime = "Etc/UTC";
export const TimezoneContext = createContext<string>(CoordinatedUniversalTime);

export interface WithTimezoneProps {
  timezone: string;
}

// WithTimezone wraps a class component to provide the
// context's timezone value as a component prop.
export function WithTimezone<T>(
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Component: React.ComponentType<T & WithTimezoneProps>,
) {
  return (props: React.PropsWithChildren<T>) => {
    // This lambda is a React function component.
    // It is safe to call a hook here.
    // eslint-disable-next-line
    const timezone = useContext(TimezoneContext);
    return <Component timezone={timezone} {...props} />;
  };
}
