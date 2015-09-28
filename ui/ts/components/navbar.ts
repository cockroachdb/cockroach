// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/lodash/lodash.d.ts" />
/// <reference path="../util/property.ts" />

// Author: Matt Tracy (matt@cockroachlabs.com)

module Components {
  "use strict";

  /**
   * NavigationBar is a general purpose component which generates a list of
   * links. One of the links can be active.
   */
  export module NavigationBar {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    export interface Target {
      view: string | MithrilVirtualElement | MithrilVirtualElement[];
      route: string;
      liClass?: string;
    }

    export interface TargetSet {
      baseRoute: string;
      targets: Utils.ReadOnlyProperty<Target[]>;
      /**
       * isActive is a function which determines whether a given target is
       * active.
       */
      isActive: (t: Target) => boolean;
    }

    export function controller(): any {}

    export function view(ctrl: any, args: {ts: TargetSet; }): _mithril.MithrilVirtualElement {
      return m(
        "ul.nav",
        _.map(args.ts.targets(), (t: Target) =>
          m("li",
            {
              className: (args.ts.isActive(t) ? "active" : "") + (t.liClass ? " " + t.liClass : " normal")
            },
            m("a",
              {
                config: m.route,
                href: args.ts.baseRoute + t.route,
              },
              t.view
              )
          )
      ));
    }
  }
}
