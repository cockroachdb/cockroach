// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { createContext, useContext } from "react";

export const CoordinatedUniversalTime = "Etc/UTC";
export const TimezoneContext = createContext<string>(CoordinatedUniversalTime);

export interface WithTimezoneProps {
  timezone: string;
}

// WithTimezone wraps a class component to provide the
// context's timezone value as a component prop.
export function WithTimezone<T>(
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
