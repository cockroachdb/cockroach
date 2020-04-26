// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import "regexp"

const startRedactable = '‹'
const startRedactableS = string(startRedactable)

var startRedactableBytes = []byte(startRedactableS)

const endRedactable = '›'
const endRedactableS = string(endRedactable)

var endRedactableBytes = []byte(endRedactableS)

const escapeMark = '?'
const escapeMarkS = string(escapeMark)

var escapeBytes = []byte(escapeMarkS)

const redactedS = startRedactableS + "×" + endRedactableS

var redactedBytes = []byte(redactedS)

var reStripSensitive = regexp.MustCompile(startRedactableS + "[^" + startRedactableS + endRedactableS + "]*" + endRedactableS)

var reStripMarkers = regexp.MustCompile("[" + startRedactableS + endRedactableS + "]")
