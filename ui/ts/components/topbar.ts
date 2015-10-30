// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  export module Topbar {
    export function controller(): void {}
    export function view(ctrl: any, args: {title: string; }): MithrilVirtualElement {
      return m(".topbar", [
        m("h2", args.title),
        m(".last-updated", [m("strong", "Updated: "), "-"]),
      ]);
    }
  }

}
