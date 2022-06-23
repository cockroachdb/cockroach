// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "sort"

type registeredCoreRule struct {
	id   int
	rule CoreRule
}

var registeredCoreRules []registeredCoreRule

type registeredBlockRule struct {
	id         int
	rule       BlockRule
	terminates []int
}

var registeredBlockRules []registeredBlockRule

type registeredInlineRule struct {
	id   int
	rule InlineRule
}

var registeredInlineRules []registeredInlineRule

type registeredPostprocessRule struct {
	id   int
	rule PostprocessRule
}

var registeredPostprocessRules []registeredPostprocessRule

func indexInt(a []int, n int) int {
	for i, m := range a {
		if m == n {
			return i
		}
	}
	return -1
}

func RegisterCoreRule(id int, rule CoreRule) {
	registeredCoreRules = append(registeredCoreRules, registeredCoreRule{
		id:   id,
		rule: rule,
	})
	sort.Slice(registeredCoreRules, func(i, j int) bool {
		return registeredCoreRules[i].id < registeredCoreRules[j].id
	})

	coreRules = coreRules[:0]
	for _, r := range registeredCoreRules {
		coreRules = append(coreRules, r.rule)
	}
}

func RegisterBlockRule(id int, rule BlockRule, terminates []int) {
	registeredBlockRules = append(registeredBlockRules, registeredBlockRule{
		id:         id,
		rule:       rule,
		terminates: terminates,
	})
	sort.Slice(registeredBlockRules, func(i, j int) bool {
		return registeredBlockRules[i].id < registeredBlockRules[j].id
	})

	blockRules = blockRules[:0]
	blockquoteTerminatedBy = blockquoteTerminatedBy[:0]
	listTerminatedBy = listTerminatedBy[:0]
	referenceTerminatedBy = referenceTerminatedBy[:0]
	paragraphTerminatedBy = paragraphTerminatedBy[:0]
	for _, r := range registeredBlockRules {
		blockRules = append(blockRules, r.rule)
		if indexInt(r.terminates, 400) != -1 {
			blockquoteTerminatedBy = append(blockquoteTerminatedBy, r.rule)
		}
		if indexInt(r.terminates, 600) != -1 {
			listTerminatedBy = append(listTerminatedBy, r.rule)
		}
		if indexInt(r.terminates, 700) != -1 {
			referenceTerminatedBy = append(referenceTerminatedBy, r.rule)
		}
		if indexInt(r.terminates, 1100) != -1 {
			paragraphTerminatedBy = append(paragraphTerminatedBy, r.rule)
		}
	}
}

func RegisterInlineRule(id int, rule InlineRule) {
	registeredInlineRules = append(registeredInlineRules, registeredInlineRule{
		id:   id,
		rule: rule,
	})
	sort.Slice(registeredInlineRules, func(i, j int) bool {
		return registeredInlineRules[i].id < registeredInlineRules[j].id
	})

	inlineRules = inlineRules[:0]
	for _, r := range registeredInlineRules {
		inlineRules = append(inlineRules, r.rule)
	}
}

func RegisterPostprocessRule(id int, rule PostprocessRule) {
	registeredPostprocessRules = append(registeredPostprocessRules, registeredPostprocessRule{
		id:   id,
		rule: rule,
	})
	sort.Slice(registeredPostprocessRules, func(i, j int) bool {
		return registeredPostprocessRules[i].id < registeredPostprocessRules[j].id
	})

	postprocessRules = postprocessRules[:0]
	for _, r := range registeredPostprocessRules {
		postprocessRules = append(postprocessRules, r.rule)
	}
}

func init() {
	RegisterCoreRule(100, ruleInline)
	RegisterCoreRule(200, ruleLinkify)
	RegisterCoreRule(300, ruleReplacements)
	RegisterCoreRule(400, ruleSmartQuotes)

	RegisterBlockRule(100, ruleTable, []int{1100, 700})
	RegisterBlockRule(200, ruleCode, nil)
	RegisterBlockRule(300, ruleFence, []int{1100, 700, 400, 600})
	RegisterBlockRule(400, ruleBlockQuote, []int{1100, 700, 400, 600})
	RegisterBlockRule(500, ruleHR, []int{1100, 700, 400, 600})
	RegisterBlockRule(600, ruleList, []int{1100, 700, 400})
	RegisterBlockRule(700, ruleReference, nil)
	RegisterBlockRule(800, ruleHeading, []int{1100, 700, 400})
	RegisterBlockRule(900, ruleLHeading, nil)
	RegisterBlockRule(1000, ruleHTMLBlock, []int{1100, 700, 400})
	RegisterBlockRule(1100, ruleParagraph, nil)

	RegisterInlineRule(100, ruleText)
	RegisterInlineRule(200, ruleNewline)
	RegisterInlineRule(300, ruleEscape)
	RegisterInlineRule(400, ruleBackticks)
	RegisterInlineRule(500, ruleStrikeThrough)
	RegisterInlineRule(600, ruleEmphasis)
	RegisterInlineRule(700, ruleLink)
	RegisterInlineRule(800, ruleImage)
	RegisterInlineRule(900, ruleAutolink)
	RegisterInlineRule(1000, ruleHTMLInline)
	RegisterInlineRule(1100, ruleEntity)

	RegisterPostprocessRule(100, ruleBalancePairs)
	RegisterPostprocessRule(200, ruleStrikethroughPostprocess)
	RegisterPostprocessRule(300, ruleEmphasisPostprocess)
	RegisterPostprocessRule(400, ruleTextCollapse)
}
