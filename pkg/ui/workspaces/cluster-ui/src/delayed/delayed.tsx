// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";
import React, { useState, useEffect } from "react";

type Props = {
  children: React.ReactElement;
  delay?: moment.Duration;
};

export const Delayed = ({
  children,
  delay = moment.duration(10, "s"),
}: Props): React.ReactElement => {
  const [isShown, setIsShown] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      setIsShown(true);
    }, delay.asMilliseconds());
    return () => clearTimeout(timer);
  }, [delay]);

  return isShown ? children : null;
};
