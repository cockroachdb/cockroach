// source: pages/account.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../components/topbar.ts"/>
/// <reference path="../components/navbar.ts"/>
/// <reference path="../util/convert.ts"/>
/// <reference path="../models/helpus.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";

  /**
   * HelpUs allows users to modify their usage reporting settings
   */
  export module HelpUs {

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
              `CockroachDB is in beta, and we're working diligently to make it better.
              Opt in to share basic anonymous usage statistics.`,
            ];
          } else {
            return ["We appreciate your support. ",
            "Reach out to us on ",
              m("a", {href: "https://gitter.im/cockroachdb/cockroach"}, "Gitter"),
              " or our ",
              m("a", {href: "https://groups.google.com/forum/#!forum/cockroachdb-users"}, "Google Group"),
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
            this.userData.showRequired = false;
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
          } else {
            this.userData.showRequired = true;
          }
        }
      }

      export function controller(): HelpUsController {
        return new HelpUsController();
      }

      export function view(ctrl: HelpUsController): _mithril.MithrilVirtualElement {
        return m(".page.help-us", [
          m.component(Components.Topbar, {title: "Help Cockroach Labs", updated: Utils.Convert.MilliToNano(Date.now()), hideHealth: true}),
          m(".section", [
            m(".header", m("h1", "Usage Reporting")),
            m(".form", [
              m(".intro", ctrl.text()),
              m("hr"),
              m("form", ctrl.userData.bindForm(), [
                m("input[name=firstname]", {placeholder: "First Name", value: ctrl.userData.attributes.firstname}), m("span.status"),
                m("input[name=lastname]", {placeholder: "Last Name", value: ctrl.userData.attributes.lastname}), m("span.status"),
                m("input[name=email][type=email][required=true]" + (ctrl.userData.showRequired ? ".show-required" : ""), {placeholder: "Email*", value: ctrl.userData.attributes.email}), m("span.status"),
                m("input[name=company]", {placeholder: "Company", value: ctrl.userData.attributes.company}), m("span.status"),
                m("", [
                  m("input[type=checkbox][name=optin]" + (ctrl.userData.savedAttributes.optin ? "" : "[required=true]"), {id: "optin", checked: ctrl.userData.attributes.optin}),
                  m("label", {for: "optin"}, "Share data with Cockroach Labs"),
                ]),
                m(".optin-text", [`By enabling this feature, you are agreeing to send us anonymous,
                  aggregate information about your running CockroachDB cluster,
                  which may include capacity and usage, server and storage device metadata, basic network topology,
                  and other information that helps us improve our products and services. We never collect any of
                  the actual data that you store in your CockroachDB cluster.
                  Except as set out above, our `, m("a", {href: "/assets/privacyPolicy.html", target: "_blank"}, "Privacy Policy"), ` governs our collection
                  and use of information from users of our products and services.`, ]),
                m("", ctrl.userData.savedAttributes.updates ? [m(".email-message", "")] : [
                  m("input[type=checkbox][name=updates]", {id: "updates", checked: ctrl.userData.attributes.updates}),
                  m("label", {for: "updates"}, "Send me product and feature updates"),
                ]),
                m("button.right", {onclick: ctrl.submit.bind(ctrl)}, "Submit"),
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
