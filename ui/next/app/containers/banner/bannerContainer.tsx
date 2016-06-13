/// <reference path="../../../typings/index.d.ts" />
import * as React from "react";
import DisconnectedBanner from "./disconnectedBanner";
import HelpusBanner from "./helpusBanner";

/**
 * This is the outer component for all banners
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div id="banner">
      <DisconnectedBanner />
      <HelpusBanner />
    </div>;
  }
}
