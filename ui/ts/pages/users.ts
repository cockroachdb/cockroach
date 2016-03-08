// source: pages/users.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/api.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";
  export module Users {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import TableData = Components.Table.TableData;
    import MithrilController = _mithril.MithrilController;
    import Property = Utils.Property;
    import Prop = Utils.Prop;
    import User = Models.Proto.User;
    import Users = Models.Proto.Users;

    // Page listing all the databases
    export module Page {
      class UserListController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<Users> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tableData: Property<TableData<string>> = Prop({
          columns: Prop([
            {
              title: "User",
              view: _.identity,
            },
          ]),
          rows: Utils.Computed(this.data, (data: Users): string[] => _.map<User, string>(data.users, "username")),
        });

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): UserListController {
        let ctrl: UserListController = new UserListController();
        Models.API.users().then(ctrl.data).then((): void => { ctrl.updated(Date.now()); });
        return ctrl;
      }

      export function view(ctrl: UserListController): MithrilVirtualElement {
        return m(".page", [
          m.component(Components.Topbar, {title: "Users", updated: Utils.Convert.MilliToNano(ctrl.updated()) }),
          m(".section",
            m(
              ".sql-table", [
                Components.Table.create(ctrl.tableData()),
                m(
                  "a.toggle",
                  {onclick: function(): void { ctrl.toggleDisplayJSON(); } },
                  ctrl.toggleText()
                ),
                m(
                  "pre",
                  {style: {display: ctrl.displayJSON ? null : "none"}},
                  JSON.stringify(ctrl.data(), null, 2)
                ),
              ]
            )
          ),
        ]);
      }
    }

  }
}
