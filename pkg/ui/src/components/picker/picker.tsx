// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Calendar as AntdCalendar, TimePicker as AntdTimePicker } from "antd";
import { CalendarProps } from "antd/lib/calendar";
import { TimePickerProps } from "antd/lib/time-picker";
import * as React from "react";
import "./picker.styl";

export const TimePicker = (props: TimePickerProps) => (
  <div className="crl-picker-container">
    <AntdTimePicker {...props} />
  </div>
);

export const Calendar = (props: CalendarProps) => (
  <div className="crl-picker-container">
    <AntdCalendar {...props} />
  </div>
);
