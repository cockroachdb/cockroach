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
    // Note: the order the banners appear here dictates their precedence
    return <div id="banner">
      <DisconnectedBanner />
      <OutdatedBanner />
      <HelpusBanner />
    </div>;
  }
}
