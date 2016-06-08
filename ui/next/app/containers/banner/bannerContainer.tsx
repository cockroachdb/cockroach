import * as React from "react";
import DisconnectedBanner from "./disconnectedBanner";

/**
 * This is the outer component for all banners
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div id="banner">
      <DisconnectedBanner />
    </div>;
  }
}
