// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";

import aegisKeyArt from "assets/signup-panel-art.png";

import "./aegisBanner.styl";

// AEGIS_URL points at the Aegis product page.
const AEGIS_URL = "https://www.cockroachlabs.com/product/aegis/";

// localStorage key recording that the user dismissed the Aegis banner, so it
// stays dismissed across reloads and sessions on this browser.
const AEGIS_DISMISSED_KEY = "aegis_banner/dismissed";

// Reading localStorage can throw (private mode, disabled, quota); treat any
// failure as "not dismissed" so the banner still renders.
function isAegisBannerDismissed(): boolean {
  try {
    return localStorage.getItem(AEGIS_DISMISSED_KEY) === "true";
  } catch {
    return false;
  }
}

// AegisBanner is the top-of-overview call to action for Aegis, the cluster ops
// agent: a rounded black card with the brand blob key-art bleeding off the
// right, a light Geist headline with a bio-green accent, and a purple CTA to
// the Aegis product page. It sits in the prominent top slot on the overview,
// above the demoted release-notes email banner. Dismissal persists via
// localStorage, so it stays dismissed across reloads and sessions.
export function AegisBanner(): React.ReactElement | null {
  const [dismissed, setDismissed] = useState(isAegisBannerDismissed);
  if (dismissed) {
    return null;
  }

  const dismiss = () => {
    try {
      localStorage.setItem(AEGIS_DISMISSED_KEY, "true");
    } catch {
      // Ignore: dismissal then only lasts for the current view.
    }
    setDismissed(true);
  };

  return (
    <section className="section">
      <div className="aegis-banner">
        {/* Blob key-art on the right, flipped inward with a gentle zoom and a
            left-edge fade so it dissolves into the black before the headline. */}
        <div className="aegis-banner__art" aria-hidden="true">
          <div className="aegis-banner__art-zoom">
            <img src={aegisKeyArt} alt="" className="aegis-banner__art-img" />
          </div>
        </div>

        <button
          type="button"
          aria-label="Dismiss"
          className="aegis-banner__dismiss"
          onClick={dismiss}
        >
          &times;
        </button>

        <div className="aegis-banner__content">
          <h2 className="aegis-banner__headline">
            Meet Aegis,{" "}
            <span className="aegis-banner__accent">
              your cluster&rsquo;s ops agent
            </span>
          </h2>
          <p className="aegis-banner__body">
            Aegis surfaces cluster issues before they page you and walks you
            through the fix.
          </p>
          <a
            className="aegis-banner__cta"
            href={AEGIS_URL}
            target="_blank"
            rel="noopener noreferrer"
          >
            Connect to Aegis
            <span className="aegis-banner__cta-arrow" aria-hidden="true">
              &rarr;
            </span>
          </a>
        </div>
      </div>
    </section>
  );
}
