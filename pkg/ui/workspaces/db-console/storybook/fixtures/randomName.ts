// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import _ from "lodash";

export function randomName(): string {
  // Add more! Have fun.
  const adjectives = [
    "assured",
    "beneficial",
    "bulky",
    "careless",
    "content",
    "convincing",
    "curious",
    "despicable",
    "emotional",
    "grumpy",
    "happy",
    "hypnotic",
    "joyous",
    "kind",
    "neglected",
    "pathetic",
    "personal",
    "pompous",
    "purple",
    "recent",
    "ruthless",
    "sneezy",
    "spacey",
    "surly",
  ];

  // Add more! Go for it.
  const nouns = [
    "goldfinch",
    "hawk",
    "hippo",
    "moose",
    "pteranodon",
    "raccoon",
    "shark",
    "turkey",
  ];

  return _.join([_.sample(adjectives), _.sample(nouns), _.random(1, 42)], "_");
}
