// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ColorBackgroundBase,
  ColorBaseBlue,
  ColorBaseDanger,
  ColorBaseInfo,
  ColorBaseSuccess,
  ColorBaseWarning,
  ColorCoreBlue4,
  ColorCoreNeutral1,
  ColorCoreNeutral2,
  ColorCoreNeutral3,
  ColorCoreNeutral6,
  ColorCoreRed4,
  ColorFont2,
  ColorFont3,
  ColorFont4,
  ColorIntentInfo1,
  TypeFamilyCode,
  TypeFamilyUi,
} from "@cockroachlabs/design-tokens";

import type { ThemeConfig } from "antd";

/** Setting the AntD theme allows us to customize styling so that AntD
 * components match CRL style guidelines.
 *
 * @see https://ant.design/docs/react/customize-theme */
export const crlTheme: ThemeConfig = {
  token: {
    borderRadius: 2,
    colorBgLayout: ColorBackgroundBase,
    colorBgSpotlight: ColorCoreNeutral6,
    colorBorder: ColorCoreNeutral3,
    colorBorderSecondary: ColorCoreNeutral2,
    colorFill: ColorCoreNeutral3,
    colorFillSecondary: ColorCoreNeutral2,
    colorFillTertiary: ColorCoreNeutral1,
    colorFillQuaternary: ColorCoreNeutral1,
    colorError: ColorBaseDanger,
    colorInfo: ColorBaseInfo,
    colorPrimary: ColorBaseBlue,
    colorSuccess: ColorBaseSuccess,
    colorWarning: ColorBaseWarning,
    colorText: ColorFont2,
    colorTextSecondary: ColorFont3,
    colorTextTertiary: ColorFont4,
    colorTextQuaternary: ColorFont4,
    // The trailing CC is 80% opacity.
    // https://gist.github.com/lopspower/03fb1cc0ac9f32ef38f4
    colorBgMask: `${ColorCoreNeutral6}CC`,
    fontFamily: TypeFamilyUi,
    fontFamilyCode: TypeFamilyCode,
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
      controlItemBgHover: ColorIntentInfo1,
      controlItemBgActive: ColorIntentInfo1,
      controlItemBgActiveHover: ColorIntentInfo1,
      colorError: ColorCoreRed4,
    },
    Table: {
      colorFillAlter: "transparent",
      colorFillSecondary: "transparent",
      colorFillContent: "transparent",
    },
    Select: {
      borderRadius: 3,
      controlOutline: `${ColorCoreBlue4}66`,
      controlItemBgHover: "transparent",
    },
    Tabs: {
      lineHeight: 1.5,
      colorPrimaryActive: ColorBaseBlue,
      colorPrimaryText: ColorBaseBlue,
      colorPrimaryTextHover: ColorBaseBlue,
      colorPrimaryHover: ColorBaseBlue,
      fontSize: 16,
      colorBorderSecondary: ColorCoreNeutral3,
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
