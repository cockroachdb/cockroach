// source: components/helpusprompt.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/helpus.ts" />
/// <reference path="modal.ts" />
/// <reference path="banner.ts" />

module Components {
  "use strict";
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
          if (Models.HelpUs.helpUsFlag() && this.userData.showHelpUs()) {
            this.stage = Stage.Banner;
            this.show = true;
            m.redraw(); // the banner wasn't being drawn on the helpus page, probably because this promise is used twice
          }
        });
      }

      // When the banner button is clicked, fade in the modal
      clickBanner(): void {
        // Adds the modal with opacity 0.
        this.stage = Stage.ModalStart;
        // set optin value to correspond with pre-checked input in modal
        this.userData.attributes.optin = true;
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
          this.userData.showRequired = false;
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
        } else {
          this.userData.showRequired = true;
        }
      }

      hide: () => void = (): void => {
        this.show = false;
        m.redraw();
      };

      fadeOut: () => void = (): void => {
        // Fade the correct modal.
        if (this.stage < Stage.ModalClose) {
          this.stage = Stage.ModalClose;
        } else if (this.stage >= Stage.ModalClose) {
          this.stage = Stage.ThanksClose;
        }
        setTimeout(this.hide, 200); // remove after animation completes
      };

      // If the close button is hit or the escape button is pressed before the
      // form was submitted, then we save with optin as false
      // and increment the number of dismisses.
      dismiss: () => void = (): void => {
        this.userData.attributes.dismissed = (this.userData.attributes.dismissed ? this.userData.attributes.dismissed + 1 : 1);
        this.userData.attributes.optin = false;
        this.userData.save()
          .then(this.fadeOut)
          .catch(this.fadeOut);
      };

      // If the close button or escape key are pressed after saving, we simply
      // close the modal.
      close: () => void = (): void => {
        this.fadeOut();
      };
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

      if (ctrl.show) {
          // Banner
        switch (ctrl.stage) {
          case Stage.Banner:
            return m.component(Components.Banner, {
              bannerClass: ".helpus" + (ctrl.userData.showHelpUs() ? ".expanded" : ".hidden"),
              content: [
                m("span.checkmark", m.trust("&#x2713; ")),
                "Help us improve. Opt in to share usage reporting statistics.",
                m("button", {onclick: ctrl.clickBanner.bind(ctrl)}, "Opt In"),
              ],
              onclose: ctrl.dismiss,
            });
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
                Opt in to share basic, anonymous usage statistics.`),
                m("form", ctrl.userData.bindForm(), [
                  m(".inputs", [
                    m("input[name=firstname]", {placeholder: "First Name"}), m("span.status"),
                    m("input[name=lastname]", {placeholder: "Last Name"}), m("span.status"),
                    m("input[name=email][type=email][required=true]" + (ctrl.userData.showRequired ? ".show-required" : ""), {placeholder: "Email*"}), m("span.status"),
                    m("input[name=company]", {placeholder: "Company (optional)"}), m("span.status"),
                    m("", [
                      m("input[type=checkbox][name=optin][required=true]", {id: "optin", checked: true}),
                      m("label", {for: "optin"}, "Share data with Cockroach Labs"),
                    ]),
                    m(".optin-text", [`By enabling this feature, you are agreeing to send us anonymous,
                      aggregate information about your running CockroachDB cluster,
                      which may include capacity and usage, server and storage device metadata, basic network topology,
                      and other information that helps us improve our products and services. We never collect any of
                      the actual data that you store in your CockroachDB cluster.
                      Except as set out above, our `, m("a", {href: "/assets/privacyPolicy.html", target: "_blank"}, "Privacy Policy"), ` governs our collection
                      and use of information from users of our products and services.`, ]),
                    m("", [
                      m("input[type=checkbox][name=updates]", {id: "updates"}),
                      m("label", {for: "updates"}, "Send me product and feature updates"),
                    ]),
                  ]),
                  m("button.right", {onclick: ctrl.modalSubmit.bind(ctrl)}, "Submit"),
                  m("button.right.cancel", {onclick: ctrl.dismiss}, "Cancel"),
                ]),
              ],
              onclose: ctrl.dismiss,
            });

          // Thanks
          case Stage.ThanksText:
          case Stage.ThanksClose:
            return m.component(Components.Modal, {
              containerClass: containerClass,
              modalClass: ".thanks" + modalClass,
              title: "Thanks for opting in!",
              content: [
                m(".thanks", [
                  "To change these settings or opt out at any time, go to the ",
                  m("a[href='/help-us/reporting']", {config: m.route, onclick: close}, "usage reporting page"),
                  ".",
                ]),
                m("button", {onclick: ctrl.close}, "Return to Dashboard"),
              ],
              onclose: ctrl.close,
            });
          default:
            console.error("Unknown stage ", ctrl.stage);
            return m("");
        }
      } else {
        return m("");
      }
    }
  }
}
