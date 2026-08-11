// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ColorBaseDanger,
  ColorBaseSuccess,
  ColorBaseWarning,
  ColorCoreRed4,
} from "@cockroachlabs/design-tokens";

import type { ThemeConfig } from "antd";

// 2026 brand overrides (pass 1). Kept local so AntD-rendered UI re-skins without
// waiting on a @cockroachlabs/design-tokens release. Values mirror
// core/_brand.scss. Purple is scarce (brand); controls/links/info ride blue-700.
const brand = {
  purple500: "#9059ff", // CTA / brand (unused by AntD colorPrimary in pass 1)
  blue700: "#127ea5", // control / link / active / info (AA 4.6:1)
  blue100: "#dffbff", // selected/hover bg
  warmGray100: "#f9f8f3",
  warmGray200: "#f2f1ee",
  warmGray300: "#e5e4dc",
  warmGray500: "#949187",
  ink: "#3a3833", // secondary text (neutral-7)
  inkStrong: "#1c221c", // primary text / titles (neutral-8)
  inkMuted: "#6b685f", // tertiary text (neutral-6)
  black6: "#101913",
  fontUi:
    'Geist-Regular, ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
  fontCode:
    'GeistMono-Regular, ui-monospace, "SF Mono", Menlo, Monaco, Consolas, monospace',
};

/** Setting the AntD theme allows us to customize styling so that AntD
 * components match CRL style guidelines.
 *
 * @see https://ant.design/docs/react/customize-theme */
export const crlTheme: ThemeConfig = {
  token: {
    borderRadius: 2,
    colorBgLayout: brand.warmGray100,
    colorBgSpotlight: brand.black6,
    colorBorder: brand.warmGray300,
    colorBorderSecondary: brand.warmGray200,
    colorFill: brand.warmGray300,
    colorFillSecondary: brand.warmGray200,
    colorFillTertiary: brand.warmGray100,
    colorFillQuaternary: brand.warmGray100,
    colorError: ColorBaseDanger,
    colorInfo: brand.blue700,
    colorPrimary: brand.blue700,
    colorSuccess: ColorBaseSuccess,
    colorWarning: ColorBaseWarning,
    colorText: brand.inkStrong,
    colorTextSecondary: brand.ink,
    colorTextTertiary: brand.inkMuted,
    colorTextQuaternary: brand.warmGray500,
    // The trailing CC is 80% opacity.
    // https://gist.github.com/lopspower/03fb1cc0ac9f32ef38f4
    colorBgMask: `${brand.black6}CC`,
    fontFamily: brand.fontUi,
    fontFamilyCode: brand.fontCode,
    fontSizeHeading1: 28,
    fontSizeHeading2: 24,
    fontSizeHeading3: 20,
    fontSizeHeading4: 16,
    fontSizeHeading5: 16,
    fontSizeLG: 14,
    lineHeight: 1.7,
    lineHeightHeading1: 1.7,
    lineHeightHeading2: 2,
    lineHeightHeading3: 1.6,
    lineHeightHeading4: 1.5,
    lineHeightHeading5: 1.5,
    controlHeight: 40, // Should match $crl-input-height.
  },
  components: {
    Modal: {
      padding: 0,
      paddingContentHorizontal: 0,
      paddingContentHorizontalSM: 0,
      paddingContentHorizontalLG: 0,
      paddingContentVertical: 0,
      paddingContentVerticalSM: 0,
      paddingContentVerticalLG: 0,
      paddingLG: 0,
      paddingMD: 0,
      paddingSM: 0,
      paddingXL: 0,
      paddingXS: 0,
      paddingXXS: 0,
    },
    Dropdown: {
      controlItemBgHover: brand.blue100,
      controlItemBgActive: brand.blue100,
      controlItemBgActiveHover: brand.blue100,
      colorError: ColorCoreRed4,
    },
    Table: {
      colorFillAlter: "transparent",
      colorFillSecondary: "transparent",
      colorFillContent: "transparent",
    },
    Select: {
      borderRadius: 3,
      controlOutline: `${brand.blue700}66`,
      controlItemBgHover: "transparent",
    },
    Tabs: {
      lineHeight: 1.5,
      colorPrimaryActive: brand.blue700,
      colorPrimaryText: brand.blue700,
      colorPrimaryTextHover: brand.blue700,
      colorPrimaryHover: brand.blue700,
      fontSize: 16,
      colorBorderSecondary: brand.warmGray300,
    },
    Pagination: {
      controlHeightSM: 24,
      colorBgTextHover: "transparent",
      colorBgTextActive: "transparent",
    },
    Tooltip: {
      controlHeight: 32,
    },
    Radio: {
      radioSize: 16,
      dotSize: 16,
      borderRadius: 6,
    },
    Checkbox: {
      controlInteractiveSize: 16,
    },
  },
};
