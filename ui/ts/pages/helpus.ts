// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../models/helpus.ts" />

module AdminViews {
  "use strict";

  export module SubModules {
    /**
     * HelpUs is used for the sign up flow to capture user information
     */
    export module HelpUs {
      import UserOptIn = Models.HelpUs.UserOptIn;
      import showHelpUs = Models.HelpUs.showHelpUs;
      import MithrilPromise = _mithril.MithrilPromise;

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
        userData: UserOptIn = new UserOptIn();

        // Current stage in the "Help Us" flow
        stage: Stage = Stage.Unknown;
        show: boolean = false;

        // The closeOnEscape function, but bound to the correct `this`.
        closeOnEscapeBound: (e: KeyboardEvent) => void;

        constructor() {
          // Adds an event listener for the escape key.

          // Bind the closeOnEscape function.
          this.closeOnEscapeBound = this.closeOnEscape.bind(this);
          document.addEventListener("keydown", this.closeOnEscapeBound);

          m.sync([
            <MithrilPromise<any>>showHelpUs(),
            <MithrilPromise<any>>this.userData.loadPromise,
          ]).then((promises: any[]): void => {
            if (promises[0]) {
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

        // Removes the event listener if the constructor is unloaded.
        onunload(): void {
          document.removeEventListener("keydown", this.closeOnEscapeBound);
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
          this.userData.attributes.dismissed = true;
          this.userData.save().then(() => {
            // Remove the event listener that listens for the escape key.
            document.removeEventListener("keydown", this.closeOnEscapeBound);
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
                m.route(route, params);
              },
              200);
          });
        }

        // Runs the close function when the escape key is pressed.
        closeOnEscape(e: KeyboardEvent): void {
          if (e.keyCode === 27) {
            this.close();
            m.redraw();
          }
        }
      }

      export function controller(): ModalController {
        return new ModalController();
      }

      export function view(ctrl: ModalController): _mithril.MithrilVirtualElement {
        let modalClass: string = "";
        let containerClass: string = "";

        if (ctrl.stage === Stage.ThanksSize) {
          modalClass += ".thanks-pre";
        }
        if (ctrl.stage === Stage.ModalStart) {
          modalClass += ".hide-modal";
        }
        if (ctrl.stage === Stage.ModalClose || ctrl.stage === Stage.ThanksClose) {
          containerClass += ".close";
        }

        let close: () => void = ctrl.close.bind(ctrl);

        if (ctrl.show) {
          // Banner
          if (ctrl.stage === Stage.Banner) {
            return m(".banner", [
              m("span.checkmark", m.trust("&#x2713; ")),
              "Check for updates and help us improve.",
              m("button", {onclick: ctrl.clickBanner.bind(ctrl)}, "Opt In"),
              m(".close", {onclick: close}, m.trust("&#x2715; ")),
            ]);
          // Help Us Form
          } else if (ctrl.stage === Stage.Modal || ctrl.stage === Stage.ThanksSize || ctrl.stage === Stage.ModalStart || ctrl.stage === Stage.ModalClose) {
            return m(".modal-container" + containerClass, [
              m(".screen", {onclick: close}),
              m(".modal" + modalClass, [
                m(".close", {onclick: close}, m.trust("&#x2715; ")),
                m("h1", "Help Make CockroachDB Better"),
                m(".intro", "Opt-in to our usage reporting by sending us basic anonymous usage statistics that will help make CockroachDB better." +
                  "You'll be able to report bugs and submit support requests directly." +
                  " We'll also let you know as soon as a new version of CockroachDB is available."),
                m("form", ctrl.userData.bindForm(), [
                  m(".inputs", [
                    m("input[name=firstname][required=true]", {placeholder: "First Name"}), m("span.status"), m("span.icon"),
                    m("input[name=lastname][required=true]", {placeholder: "Last Name"}), m("span.status"), m("span.icon"),
                    m("input[name=email][type=email][required=true]", {placeholder: "Email"}), m("span.status"), m("span.icon"),
                    m("input[name=company]", {placeholder: "Company (optional)"}), m("span.status"), m("span.icon"),
                    m("", [
                      m("input[type=checkbox][name=updates]", {id: "updates"}),
                      m("label", {for: "updates"}, "Send me product and feature updates."),
                    ]),
                  ]),
                  m("button", {onclick: ctrl.modalSubmit.bind(ctrl)}, "Submit"),
                ]),
              ]),
            ]);
          // Thanks
          } else if (ctrl.stage === Stage.ThanksText || ctrl.stage === Stage.ThanksClose) {
            return m(".modal-container" + containerClass, [
              m(".screen", {onclick: close}),
              m(".modal.thanks" + modalClass, [
                m(".close", {onclick: close}, m.trust("&#x2715; ")),
                m("h1", "Thanks for signing up!"),
                m(".thanks", [
                  "To report bugs, submit support requests, or change these settings at any time, go to the ",
                  m("a[href='/settings/support']", {config: m.route}, "support page"),
                  ".",
                ]),
                m("button", {onclick: close}, "Return to Dashboard"),
              ]),
            ]);
          }
        }
      }
    }
  }
}
