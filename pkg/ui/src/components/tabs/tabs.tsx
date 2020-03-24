// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tabs as AntdTabs } from "antd";
import { TabPaneProps, TabsProps } from "antd/lib/tabs";
import * as React from "react";
import "./tabs.styl";

export interface ITabsProps {
  children?: React.ReactNode;
}

export function Tabs({ children, ...props }: TabsProps & ITabsProps) {
  return (
    <AntdTabs className="crl-tabs" {...props}>
      {children}
    </AntdTabs>
  );
}

export const TabPane = ({ children, ...props }: TabPaneProps & ITabsProps) => (
  <AntdTabs.TabPane {...props}>
    {children}
  </AntdTabs.TabPane>
);
