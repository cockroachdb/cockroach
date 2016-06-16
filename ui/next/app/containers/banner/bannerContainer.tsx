/// <reference path="../../../typings/index.d.ts" />
import * as React from "react";
import DisconnectedBanner from "./disconnectedBanner";
import HelpusBanner from "./helpusBanner";
import OutdatedBanner from "./outdatedBanner";

/**
 * This is the outer component for all banners
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div id="banner">
      {/* Note: the order the banners appear here dictates their precedence */}
      <DisconnectedBanner />
      <OutdatedBanner />
      <HelpusBanner />
    </div>;
  }
}
