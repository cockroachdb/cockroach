// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Exports icons as SVG strings.
import React from "react";

export const leftArrow = `<svg width="7px" height="10px" viewBox="0 0 7 10" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
  <!-- Generator: Sketch 41.2 (35397) - http://www.bohemiancoding.com/sketch -->
  <title>Path 2</title>
  <desc>Created with Sketch.</desc>
  <defs></defs>
  <g id="Symbols" stroke="none" stroke-width="1" fill="none" fill-rule="evenodd">
    <g id="View-button" transform="translate(-18.000000, -15.000000)" stroke-width="2" stroke="#51BBFD">
      <g id="Path-2">
        <polyline points="24 16 20 20 24 24"></polyline>
      </g>
    </g>
  </g>
</svg>`;

export const rightArrow = `<svg width="7px" height="10px" viewBox="0 0 7 10" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
    <!-- Generator: Sketch 41.2 (35397) - http://www.bohemiancoding.com/sketch -->
    <title>Path 2 Copy</title>
    <desc>Created with Sketch.</desc>
    <defs></defs>
    <g id="Symbols" stroke="none" stroke-width="1" fill="none" fill-rule="evenodd">
      <g id="View-button" transform="translate(-241.000000, -15.000000)" stroke-width="2" stroke="#51BBFD">
        <g id="Path-2-Copy">
          <polyline transform="translate(244.000000, 20.000000) scale(-1, 1) translate(-244.000000, -20.000000) " points="246 16 242 20 246 24"></polyline>
        </g>
      </g>
    </g>
</svg>`;

export const criticalIcon = (
  <svg
    width="20px"
    height="20px"
    viewBox="0 0 20 20"
    version="1.1"
    xmlns="http://www.w3.org/2000/svg"
    xmlnsXlink="http://www.w3.org/1999/xlink"
  >
    {/* Generator: Sketch 42 (36781) - http://www.bohemiancoding.com/sketch */}
    <title>Group 3</title>
    <desc>Created with Sketch.</desc>
    <defs></defs>
    <g
      id="Symbols"
      stroke="none"
      strokeWidth="1"
      fill="none"
      fillRule="evenodd"
    >
      <g
        id="Warning---Connection-Lost"
        transform="translate(-25.000000, -19.000000)"
      >
        <g id="Group-3">
          <g transform="translate(25.000000, 19.000000)">
            <path
              d="M8.75139389,1.26729609 C9.24631171,0.277343549 10.0519669,0.283622947 10.5438615,1.26729609 L19.1995435,18.5766161 C19.4476073,19.0726851 19.1933971,19.4748284 18.6568996,19.4748284 L0.640505826,19.4748284 C0.092748094,19.4748284 -0.15156708,19.0753209 0.0977558747,18.5766161 L8.75139389,1.26729609 Z"
              id="Rectangle-4"
              fill="#F26969"
            ></path>
            <g
              id="Group-2"
              transform="translate(8.000000, 3.000000)"
              fontSize="14"
              fontFamily="SourceSansPro-Bold, SourceSansPro-Regular"
              fill="#FFFFFF"
              fontWeight="bold"
            >
              <text id="!">
                <tspan x="0" y="14">
                  !
                </tspan>
              </text>
            </g>
          </g>
        </g>
      </g>
    </g>
  </svg>
);

export const warningIcon = (
  <svg
    width="21px"
    height="21px"
    viewBox="0 0 21 21"
    version="1.1"
    xmlns="http://www.w3.org/2000/svg"
    xmlnsXlink="http://www.w3.org/1999/xlink"
  >
    {/* Generator: Sketch 41.2 (35397) - http://www.bohemiancoding.com/sketch */}
    <title>Group 2</title>
    <desc>Created with Sketch.</desc>
    <defs></defs>
    <g
      id="Symbols"
      stroke="none"
      strokeWidth="1"
      fill="none"
      fillRule="evenodd"
    >
      <g id="Alert" transform="translate(-18.000000, -20.000000)">
        <g id="Group-4">
          <g id="Group-2" transform="translate(18.000000, 20.000000)">
            <circle
              id="Oval-5"
              fill="#FFCD02"
              cx="10.5"
              cy="10.5"
              r="10.5"
            ></circle>
            <text
              id="!"
              fontFamily="SourceSansPro-Bold, SourceSansPro-Regular"
              fontSize="14"
              fontWeight="bold"
              fill="#FFFFFF"
            >
              <tspan x="8" y="16">
                !
              </tspan>
            </text>
          </g>
        </g>
      </g>
    </g>
  </svg>
);

export const informationIcon = (
  <svg
    width="24"
    height="24"
    viewBox="0 0 24 24"
    fill=""
    xmlns="http://www.w3.org/2000/svg"
  >
    <g clipPath="url(#clip0_11029_22094)">
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M0 12C0 5.37261 5.39028 0 12 0C18.6274 0 24 5.37261 24 12C24 18.6274 18.6274 24 12 24C5.37261 24 0 18.6274 0 12ZM11.1163 18.7865H12.8836V9.52586H11.1163V18.7865ZM11.1163 7.68786H12.8836V5.21364H11.1163V7.68786Z"
        fill="#394455"
      />
    </g>
    <defs>
      <clipPath id="clip0_11029_22094">
        <rect
          width="24"
          height="24"
          fill="white"
          transform="matrix(1 0 0 -1 0 24)"
        />
      </clipPath>
    </defs>
  </svg>
);
