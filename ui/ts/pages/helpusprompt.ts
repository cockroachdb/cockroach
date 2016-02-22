// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../models/helpus.ts" />
/// <reference path="../components/modal.ts" />

module AdminViews {
  "use strict";

  export module SubModules {
    /**
     * HelpUs is used for the sign up flow to capture user information
     */
    export module HelpUsPrompt {
      import UserOptIn = Models.HelpUs.UserOptIn;

      enum Stage {
        Unknown,
        // Show the "Help Us" banner at the top of the page.
        Banner,
        // Fade in the "Sign Up" modal.
        ModalStart,
        // Show the "Sign Up" modal.
        Modal,
        // Fade out the "Sign Up" modal if it's closed without signing up.
        ModalClose,
        // Transition the modal to the "Thanks" modal size while fading out the old text.
        ThanksSize,
        // Show the "Thanks" modal with the correct size and fade in the text.
        ThanksText,
        // Fade out the "Thanks" modal.
        ThanksClose
      }

      class ModalController {
        userData: UserOptIn = Models.HelpUs.userOptInSingleton;

        // Current stage in the "Help Us" flow
        stage: Stage = Stage.Unknown;
        show: boolean = false;

        constructor() {
          this.userData.loadPromise.then((): void => {
            if (Models.HelpUs.helpUsFlag() && !this.userData.showHelpUs()) {
              this.stage = Stage.Banner;
              this.show = true;
            } else {
              let routeParts: string[] = m.route().split("?");
              let route: string = routeParts[0];
              let params: Object = m.route.parseQueryString(routeParts[1]);
              delete params["help-us"];
              m.route(route, params);
            }
          });
        }

        // When the banner button is clicked, fade in the modal
        clickBanner(): void {
          // Adds the modal with opacity 0.
          this.stage = Stage.ModalStart;
          // At the next tick, set the modal opacity to 1 and trigger a redraw so it will fade in.
          setTimeout(
            () => {
              this.stage = Stage.Modal;
              m.redraw();
            },
            0);
        }

        // Submit the modal data if the form is valid
        modalSubmit(e: Event): void {
          let target: HTMLButtonElement = <HTMLButtonElement>e.target;
          if (target.form.checkValidity()) {

            this.userData.save().then(() => {
              // Change the modal size from the "Sign Up" to the "Thanks" size
              this.stage = Stage.ThanksSize;
              // After 400ms display the "Thanks" modal text
              setTimeout(
                () => {
                  this.stage = Stage.ThanksText;
                  m.redraw();
                },
                400);
            });
          }
        }

        // If the close button is hit, or the escape button is pressed, close the modal and remove "help-us" from the URL.
        close(): void {
          this.userData.attributes.dismissed = (this.userData.attributes.dismissed ? this.userData.attributes.dismissed + 1 : 1);
          this.userData.save().then(() => {
            // Remove the event listener that listens for the escape key.
            let routeParts: string[] = m.route().split("?");
            let route: string = routeParts[0];
            let params: Object = m.route.parseQueryString(routeParts[1]);
            delete params["help-us"];
            // Fade the correct modal.
            if (this.stage < Stage.ModalClose) {
              this.stage = Stage.ModalClose;
            } else {
              this.stage = Stage.ThanksClose;
            }
            // Remove help-us from the URL after the fade completes.
            setTimeout(
              () => {
                this.show = false;
                m.route(route, params);
              },
              200);
          });
        }
      }

      export function controller(): ModalController {
        return new ModalController();
      }

      export function view(ctrl: ModalController): _mithril.MithrilVirtualElement {
        let modalClass: string = "";
        let containerClass: string = "";

        // set modal class
        if (ctrl.stage === Stage.ThanksSize) {
          modalClass = ".thanks-pre";
        } else if (ctrl.stage === Stage.ModalStart) {
          modalClass = ".hide-modal";
        }

        // set modal container class
        if (ctrl.stage === Stage.ModalClose || ctrl.stage === Stage.ThanksClose) {
          containerClass = ".close";
        }

        let close: () => void = ctrl.close.bind(ctrl);

        if (ctrl.show) {
            // Banner
          switch (ctrl.stage) {
            case Stage.Banner:
              return m(".banner", [
                m("span.checkmark", m.trust("&#x2713; ")),
                "Check for updates and help us improve",
                m("button", {onclick: ctrl.clickBanner.bind(ctrl)}, "Opt In"),
                m(".close", {onclick: close}, m.trust("&#x2715; ")),
              ]);
            // Help Us Form
            case Stage.ThanksSize:
            case Stage.ModalStart:
            case Stage.Modal:
            case Stage.ModalClose:
              return m.component(Components.Modal, {
                containerClass: containerClass,
                modalClass: modalClass,
                title: "Help Cockroach Labs",
                content: [
                  m(".intro", `CockroachDB is in beta, and we're working diligently to make it better.
                  Opt in to share basic, anonymous usage statistics,
                  and get notified when new versions of CockroachDB are available.`),
                  m("form", ctrl.userData.bindForm(), [
                    m(".inputs", [
                      m("", [
                        m("input[type=checkbox][name=optin][required=true]", {id: "optin", checked: true}),
                        m("label", {for: "optin"}, "Share data with Cockroach Labs"),
                      ]),
                      m(".optin-text", [`By enabling this feature, you are agreeing to send us anonymous,
                        aggregate information about your running CockroachDB cluster,
                        which may include capacity and usage, server and storage device metadata, basic network topology,
                        and other information that helps us improve our products and services. We never collect any of
                        the actual data that you store in your CockroachDB cluster.
                        Except as set out above, our `, m("a", {href: null}, "Privacy Policy"), ` governs our collection
                        and use of information from users of our products and services.`, ]),
                      m("input[name=email][type=email][required=true]", {placeholder: "Email"}), m("span.status"), m("span.icon"),
                      m("input[name=firstname]", {placeholder: "First Name"}), m("span.status"), m("span.icon"),
                      m("input[name=lastname]", {placeholder: "Last Name"}), m("span.status"), m("span.icon"),
                      m("input[name=company]", {placeholder: "Company (optional)"}), m("span.status"), m("span.icon"),
                      m("", [
                        m("input[type=checkbox][name=updates]", {id: "updates"}),
                        m("label", {for: "updates"}, "Send me product and feature updates."),
                      ]),
                    ]),
                    m("button", {onclick: ctrl.modalSubmit.bind(ctrl)}, "Submit"),
                    m("button", {onclick: close}, "Cancel"),
                  ]),
                ],
                onclose: close,
              });

            // Thanks
            case Stage.ThanksText:
            case Stage.ThanksClose:
              return m.component(Components.Modal, {
                containerClass: containerClass,
                modalClass: ".thanks" + modalClass,
                title: "Thanks for signing up!",
                content: [
                  m(".thanks", [
                    "To report bugs, submit support requests, or change these settings at any time, go to the ",
                    m("a[href='/settings/support']", {config: m.route}, "support page"),
                    ".",
                  ]),
                  m("button", {onclick: close}, "Return to Dashboard"),
                ],
                onclose: close,
              });
            default:
              console.error("Unknown stage ", ctrl.stage);
          }
        }
      }
    }
  }
}
