// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";

import aegisKeyArt from "assets/signup-panel-art.png";

import "./aegisBanner.scss";

// AEGIS_URL points at the Aegis product page.
const AEGIS_URL = "https://www.cockroachlabs.com/product/aegis/";

// AegisBanner is the 2026 top-of-overview call to action for Aegis, the cluster
// ops agent. It ports the design reference's brand black-card treatment: a
// rounded black card with the brand blob key-art bleeding off the right, a
// light Geist headline with a bio-green accent, and a purple CTA into Aegis
// sign-in. It takes the prominent slot the release-notes email banner used to
// hold (that banner is demoted to the bottom of the overview). Dismissal is
// session-local (resets on reload), matching the reference.
export function AegisBanner(): React.ReactElement | null {
  const [dismissed, setDismissed] = useState(false);
  if (dismissed) {
    return null;
  }

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
          onClick={() => setDismissed(true)}
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
