// source: pages/account.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../components/topbar.ts"/>
/// <reference path="../components/navbar.ts"/>
/// <reference path="../util/convert.ts"/>
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../models/helpus.ts" />

// Author: Max Lang (max@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";
  import NavigationBar = Components.NavigationBar;
  import Target = NavigationBar.Target;

  /**
   * Log is the view for exploring the logs from nodes.
   */
  export module Settings {
    /**
     * Page displays log entries from the current node.
     */

     let isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
       return ((m.route.param("detail") || "") === t.route);
     };

    export module Page {
      import MithrilVirtualElement = _mithril.MithrilVirtualElement;
      import UserOptIn = Models.HelpUs.UserOptIn;

      class HelpUsController {
        userData: UserOptIn = Models.HelpUs.userOptInSingleton;

        saving: boolean = false;
        saveFailed: boolean = false;

        text(): (MithrilVirtualElement|string)[] {
          if (!this.userData.optedIn()) {
            return [
              `Cockroach DB is in beta and we're working diligently to make it \
                better. Sign up to share feedback, submit bug reports, and get
                updates, or just hit us up on `,
              m("a", {href: "http://www.github.com/cockroachdb/cockroach"}, "Github"),
              ".",
            ];
          } else {
            return ["Thanks for signing up! We appreciate your support and feedback. ",
            "Feel free to reach out to us on ",
              m("a", {href: "http://www.github.com/cockroachdb/cockroach"}, "Github"),
              ".",
              m("br"),
              m("br"),
              "Edit your details here at any time.",
            ];
          }
        }

        // Submit the modal data if the form is valid
        submit(e: Event): void {
          let target: HTMLButtonElement = <HTMLButtonElement>e.target;
          if (target.form.checkValidity()) {
            this.saving = true;
            m.redraw();
            this.userData.save()
              .then(() => {
                this.saving = false;
                this.saveFailed = false;
              })
              .catch(() => {
                this.saving = false;
                this.saveFailed = true;
              });
          }
        }
      }

      export function controller(): HelpUsController {
        return new HelpUsController();
      }

      export function view(ctrl: HelpUsController): _mithril.MithrilVirtualElement {

        let t: Target = {view: "Support", route: "support"};

        return m(".page.registration", [
          m.component(Components.Topbar, {title: "Settings", updated: Utils.Convert.MilliToNano(Date.now())}),
          m.component(
            Components.NavigationBar,
            {ts: {
              baseRoute: "/settings/",
              targets: <Utils.ReadOnlyProperty<Target[]>>Utils.Prop([t]),
              isActive: isActive,
            },
          }),
          m(".section", [
            m(".header", m("h1", "Help Cockroach Labs")),
            m(".form", [
              m(".intro", ctrl.text()),
              m("hr"),
              m("form", ctrl.userData.bindForm(), [
                m("", [
                  m("input[type=checkbox][name=optin]" + (ctrl.userData.savedAttributes.optin ? "" : "[required=true]"), {id: "optin", checked: ctrl.userData.attributes.optin}),
                  m("label", {for: "optin"}, "Share data with Cockroach Labs"),
                ]),
                m("input[name=email][type=email][required=true]", {placeholder: "Email", value: ctrl.userData.attributes.email}), m("span.status"), m("span.icon"),
                m("input[name=firstname]", {placeholder: "First Name", value: ctrl.userData.attributes.firstname}), m("span.status"), m("span.icon"),
                m("input[name=lastname]", {placeholder: "Last Name", value: ctrl.userData.attributes.lastname}), m("span.status"), m("span.icon"),
                m("input[name=company]", {placeholder: "Company (optional)", value: ctrl.userData.attributes.company}), m("span.status"), m("span.icon"),
                m("", ctrl.userData.savedAttributes.updates ? [m(".email-message", "")] : [
                  m("input[type=checkbox][name=updates]", {id: "updates", checked: ctrl.userData.attributes.updates}),
                  m("label", {for: "updates"}, "Send me updates about Cockroach"),
                ]),
                m("hr"),
                m("button", {onclick: ctrl.submit.bind(ctrl)}, "Submit"),
                m(".saving" + (ctrl.saving ? ".no-animate" : ""),
                  {style: (ctrl.saving || ctrl.saveFailed ? "opacity: 1;" : "")},
                  (ctrl.saving ? "Saving..." : (!ctrl.saveFailed ? "Saved" : "Save Failed" ))),
              ]),
            ]),
          ]),
        ]);
      }
    }
  }
}
