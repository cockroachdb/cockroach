# Milestone Summary Prompt

Use this prompt with an LLM to generate a monthly progress summary from the JSON output of `ghsearchdump`.

**Purpose**: Create executive-friendly team updates that help leadership understand outcomes delivered, resource allocation, strategic decisions, and team health—not git activity logs.

**Recommended Model:** Claude Opus 4.5 (`claude-opus-4-5-20251101`) - exceptional at narrative writing and strategic synthesis.

---

You are helping write a monthly progress summary for engineering leadership and stakeholders.

I have JSON containing PRs worked on this month (attached). Please help me create a ~400 word narrative summary suitable for inclusion in a larger milestone document.

**Audience: Leadership consuming high-level team updates**
Leadership needs to understand: What did this team accomplish? What strategic decisions were made? Where is effort concentrated? What should I be aware of?

Write from a team portfolio perspective, not a git log:

- **Outcomes and capabilities delivered**: What can users/systems now do that they couldn't before? What milestones were reached? Lead with tangible results. When possible, cite concrete proof points: experimental results, benchmark comparisons, before/after improvements.
- **Strategic focus and effort distribution**: Where did major effort go? What were the primary workstreams? If work was preempted by higher-priority issues or team transitions, explain naturally. If onboarding or knowledge transfer consumed time, mention it. Backports are expected operational work and don't need special mention unless they revealed a strategic issue.
- **Strategic decisions and trade-offs**: What choices were made this month? What was deprioritized and why? What technical debt or shortcuts were accepted to ship faster, or what investment was made for future payoff? Explain the rationale behind architectural decisions.
- **Risks, blockers, and realistic progress**: What's harder than expected? What dependencies emerged? Be explicit about delivery risk status (AT RISK, PARTIAL) with honest context on constraints: staffing changes, scope adjustments, unexpected complexity. Don't sugarcoat—leadership needs accurate status.
- **Forward-looking implications**: What planning happened? What architectural decisions were made? What deprecations or migrations are in flight? Be specific about next steps, not vague. What needs stakeholder input or cross-team coordination?

**No individual attribution**: Leadership cares about team outcomes and resource allocation, not who committed what. Readers can click any PR or issue link to find the author if they need to follow up on specifics. Only mention individuals when their specific role or expertise is critical to understanding a decision or strategic direction (map: wenyihu6=Wenyi, tbg=Tobias, sumeerbhola=Sumeer, angeladietz=Angela).

**Technical requirements:**
- Write in narrative prose, not bullet lists or per-person summaries
- **Structure by epic/workstream**: Organize into separate paragraphs—one for each major epic or workstream. This helps leadership quickly understand progress on different initiatives.
- Sound like a human wrote it: avoid PR counts, percentages, or saying "X of Y were..." Use natural phrasing like "substantial effort", "primary focus", "considerable time"
- Use inline markdown links for Jira compatibility: `[#12345](https://github.com/cockroachdb/cockroach/pull/12345)`
- **Cite PRs comprehensively as proof points**: Each technical claim should be backed by PR links. More PR links is better—they serve as evidence that work was actually completed. Group related PRs thematically in parentheses: `([#123](url), [#456](url), [#789](url))`. This lets readers verify claims and understand the scope of work.
- Extract Epic IDs using `/Epic[:\s]+CRDB-(\d+)/i` to identify major workstreams
- Skip bot comments, CI noise, and mechanical changes
- **Output format**: Provide the final summary in a markdown code block (triple backticks with `markdown` language tag) so the user can easily copy-paste the raw markdown text with proper link formatting intact

**Tone**: Technical but accessible for cross-functional leadership. Write like you're giving a portfolio update to stakeholders who need to understand:
- What business value was delivered
- Where major engineering effort was concentrated
- What strategic bets or course corrections were made
- What risks or dependencies they should know about

This is NOT a git activity log or PR counting exercise. It's a strategic narrative helping leadership understand what the team accomplished and where effort went.

---

## Example Outputs

### Example 1: Team retrospective style

This example demonstrates the leadership perspective—focused on outcomes delivered, resource allocation, strategic decisions, and realistic progress assessment:

> Our small team's most notable output this month was work to align the existing allocator's lease-count balancing for replicas and leases with the multi-metric allocator, in order to prevent thrashing. We are pleased to report that this works.
>
> We also spent a lot of time poring over the behavior of existing simulation tests together, to understand details of both the old allocator and mma. It's a steep learning curve, we are accelerating and making many useful testing, interpretability, and observability improvements along the way. Still, we expect to continue spending significant amounts of time in this area, paying down the cognitive debt that has built up in this area of the product and making it more accessible to newcomers.
>
> We also began a small but important side quest to disable "follow the workload" lease rebalancing by default (in 26.1+), more background here. By removing this somewhat obscure feature, we would facilitate a (future) full transition off the old allocator. We still need to work through the deprecation plan, so please reach out to us if you have thoughts on this.
>
> October also included virtual and in-person planning, so significant amounts of energy went into a backlog review, as well as formulating a plan for the multi-metric allocator in 26.1 and beyond. (TLDR: cloud preview of the multi-metric allocator).
>
> We will be closing out our 25.4 epic as PARTIAL. We have met the main goal of a master non-production version of the multi-metric allocator that performs the basic advertised functions, but had hoped to have addressed a number of additional work items related to production readiness at this point. Given the substantial non-project (mainly reactive support and admission control onboarding) burden that the team had to shoulder and the thin staffing, we are content with what we have achieved.

Note how this example addresses leadership needs:
- **Delivers tangible outcomes first**: "work to align... in order to prevent thrashing. We are pleased to report that this works" (concrete capability delivered)
- **Explains business value**: "prevent thrashing", "facilitate a future full transition" (why stakeholders should care)
- **Shows strategic focus**: "spent a lot of time poring over simulation tests", "side quest to disable...", "significant amounts of energy went into... planning" (where concentrated effort went, naturally described)
- **Strategic decisions visible**: "began a small but important side quest to disable..." with deprecation context (proactive architectural choice, including rationale)
- **Realistic progress assessment**: "steep learning curve", "expect to continue spending significant amounts of time", "cognitive debt" (honest about challenges and what's still ahead)
- **Epic closure with context**: "PARTIAL" explained by "substantial non-project burden" and "thin staffing" (leadership understands constraints without PR-counting)
- **Forward-looking**: "formulating a plan for... 26.1 and beyond", "still need to work through the deprecation plan" (what's next, what needs coordination)
- **Zero individual attribution**: Team outcomes only—stakeholders care about what shipped and strategic direction, not git commits

### Example 2: Milestone with strategic context and risk transparency

This example demonstrates how to lead with accomplishments while weaving in strategic rationale and trade-offs:

> We officially finalized the "prototype and design" phase of the project by sending out the design doc; the corresponding line item in the scorecard is now completed. Because most of Tobias's planned work on MMA this month was preempted by the (ad-hoc) decommission stalls project (see below), we are moving https://cockroachlabs.atlassian.net/browse/CRDB-49117 to AT RISK. However, there is encouraging news: the skew experiment (run on master) below showcases the MMA solving an imbalance that the SMA (single-metric allocator) leaves untouched. Initially, CPU and IO are both imbalanced, but the SMA can only reason about CPU, and thus doesn't correct the IO imbalance. It is only when we additionally enable the MMA that the IO discrepancy gets resolved:
>
> [experimental visualization would appear here]
>
> The experiment is somewhat contrived (mostly to ensure that the SMA doesn't "accidentally" resolve the IO imbalance unintentionally) but it proves that the MMA works correctly and doesn't cause system instability at least in this specific situation. We will use the remaining development time to run additional experiments, scrutinize the behavior of the MMA in more scenarios, and improve the communication between the SMA and MMA (since the SMA will remain responsible for repair actions). Despite the AT RISK status, we can guarantee that some form of the MMA will be available for private testing in 25.4.

Note how Example 2 demonstrates leadership-oriented narrative:
- **Leads with milestone, not blockers**: Opens with design phase completion and scorecard item checked off—an accomplishment—before mentioning risk status
- **Explicit risk transparency with verifiable epic link**: States "AT RISK" status directly with the Jira link, giving leadership precise visibility into project health
- **Preemptions mentioned naturally**: "preempted by the (ad-hoc) decommission stalls project (see below)" acknowledges the context switch without dwelling on resource drama
- **Concrete proof points over claims**: The skew experiment provides tangible evidence—shows specific before/after behavior (SMA can't fix IO imbalance, MMA does). This is verification, not just "we made progress"
- **Counterbalances risk with guarantees**: "Despite the AT RISK status, we can guarantee that some form of the MMA will be available for private testing in 25.4" - gives leadership both honest risk assessment and what they can count on
- **Forward-looking is specific**: Not "we'll continue working" but concrete next steps: "run additional experiments, scrutinize the behavior of the MMA in more scenarios, and improve the communication between the SMA and MMA"
- **Strategic context woven throughout**: Explains why AT RISK (preemption), what the technical achievement means (solving multi-dimensional imbalances), and what constraints exist (SMA remains responsible for repairs)
- **No individual attribution except where expertise matters**: Only mentions "Tobias's planned work" when explaining the preemption's impact on a specific epic—this is contextually relevant, not git log recitation

## What Makes a Strong Summary

Good milestone summaries demonstrate these characteristics:

**Orient around PRs first, refine with context**: The merged PRs are what actually shipped—start there. Use standup notes and additional context to explain why/how, but don't let investigation drama or team logistics dominate over actual accomplishments. If standup mentions week-long debugging but no PRs merged, it's context for capacity discussion, not a primary outcome.

**Lead with milestones and accomplishments, not blockers**: Open with what shipped, what capabilities are now available, what problems were solved. Mention blockers and challenges as context for decisions or forward-looking plans, not as the main narrative. "We reached milestone X, though we discovered issue Y that will inform our January approach" beats "We got blocked by Y this month."

**Explicitly state strategic rationale**: Don't assume leadership knows why technical work matters. Explain the "why" naturally in the narrative flow. Examples:
- "Static allocation either wastes infrastructure spend (over-provisioning) or degrades customer performance (under-provisioning)"
- "A buggy span configuration could previously crash all nodes in a cluster repeatedly on restart"
- "This decision was driven by customer escalations around..."
- "We chose X over Y because..."

Avoid stilted phrases like "The rationale:" - just weave the reasoning into your sentences naturally.

**Call out trade-offs with leadership implications**: Make technical debt, shortcuts, or design compromises explicit with clear implications. "This represents a deliberate trade-off—accepting the technical debt of skipping problematic ranges rather than blocking adoption on perfect input validation. Leadership should be aware that while this makes the system safer, it means some ranges may not be rebalanced if they have configuration issues."

**Concrete proof points over claims**: Instead of "we made progress on MMA", say "the skew experiment showcases the MMA solving an IO imbalance that the single-metric allocator leaves untouched" with experimental evidence. Show before/after comparisons, benchmark results, or concrete demonstrations.

**Comprehensive PR linking as evidence**: Every technical claim should be backed by PR links. More is better—they prove work was completed and let leadership verify scope. Examples:
- ❌ Weak: "We improved error handling" (no evidence)
- ✅ Strong: "We improved error handling by converting panics to graceful degradation ([#159653](url), [#159872](url), [#159215](url), [#159123](url))"

Group related PRs thematically in parentheses. If a paragraph discusses production-hardening work, cite all relevant PRs even if some are smaller contributions—this demonstrates the breadth of effort.

**Explicit risk transparency without making unverifiable claims**: Share delivery concerns honestly: "we are moving CRDB-XXXXX to AT RISK" or "closing this epic as PARTIAL"—but only if you can verify this from epic status or team decisions. Don't infer risk status from standup discussions. Counterbalance risk disclosure with what you can guarantee.

**Natural flow, not rigid epic sections**: Organize thematically when it tells a better story. Production-hardening → Observability → Reactive work can flow better than Epic A → Epic B → Epic C. Use epic-based paragraphs when you have distinct workstreams with different outcomes. Choose based on what helps leadership understand the month's narrative.

**Preemptions mentioned naturally, not as resource drama**: "This preempted some planned MMA work but was necessary to address in-field stability" beats lengthy discussions of capacity allocation or time breakdowns. Acknowledge context switches without dwelling on them.

**Specific forward-looking statements**: Not "we'll continue working on this" but "gather real-world performance data", "identify edge cases and refine allocation strategies", "run controlled experiments in 2-3 internal clusters".

## What Additional Context Helps

**Start with the PRs, refine with context:**

The PR JSON (output.json) is your **primary source** - it shows what actually shipped, what epics work maps to, and the technical details. Always orient your summary around the merged PRs first.

**Daily standup notes (optional refinement)**: Use standup notes to **add color and context** to the PR-based narrative—what was hard, what blocked progress, strategic decisions made, preemptions, and team capacity. They help explain the story behind the work:
- Strategic pivots: "Let's pause cloud production rollout until we understand the DoorDash case"
- What consumed time: "Allocbench investigation took week+ of debugging"
- Preemptions: "Primary oncall + L3 incidents + scale test debugging"
- Knowledge transfer: "Wenyi walked Angela through constraint code"
- What was actually hard: "Brain melting. Many different concepts"
- Team capacity: Holiday schedules, people traveling, thin staffing
- Discovered blockers: "Capacity modeling bug could block GA"

**Don't let standup notes dominate** - if a standup mentions lots of investigation but no PRs merged, it's context not a primary outcome. Focus on what shipped, use standups to explain why/how.

**Epic context**: Epic goals, status (on track / AT RISK / PARTIAL), and strategic objectives. The LLM can extract epic IDs from PRs and read PR descriptions, but won't know if an epic is behind schedule or what the original goals were.

**Forward-looking plans**: What's planned for next month/quarter? What experiments will you run? What's the next milestone? This prevents generic "we'll continue working on..." statements.

**Strategic rationale**: Why did you prioritize this work? What business problem does it solve? What release is this targeting? What customer is asking for it?

**Proof points**: Experimental results, benchmark data, before/after comparisons. Concrete evidence strengthens the narrative.

**Risks and blockers**: Dependencies, technical unknowns, staffing constraints. Leadership needs accurate status for planning.

Example of providing rich context:
```
Attach: output.json
Attach: standup.txt (daily standup notes)

Additional context:
- Epic CRDB-55052: Multi-metric allocator production hardening. Was on track, now AT RISK due to capacity modeling bug discovered via allocbench mid-month.
- Strategic decision Dec 15: Paused cloud production rollout to address capacity issue. Regrouping in January.
- Major blocker: CPU capacity modeling bug - non-KV work causes underestimation. Blocks GA, consumed week+ of investigation.
- Next month: Fix capacity model, regroup on cloud rollout timeline, continue bandwidth AC rollout (Angela leading).
- Team capacity: Thin due to holidays (Sumeer out Dec 19, Angela Dec 24-Jan 2, Tobias moving + oncall).
```

## Tips for Best Results

- **Attach the JSON** file generated by `ghsearchdump` to your prompt (required)
- **Optionally attach standup notes** to add context, but remember: PRs are primary, standups refine
- **Optionally provide epic context** and forward-looking plans for a more strategic narrative
- **Ask leadership questions**: "What outcomes were delivered?", "Where did major effort go?", "What strategic decisions were made?", "What should stakeholders know?"
- **Sound human, not robotic**: Use natural language like "we spent considerable effort on..." rather than "35% of PRs were...". Avoid exact counts, percentages, or quantitative breakdowns unless truly meaningful to the narrative.
- **Iterate as needed**: Ask for condensing, different emphasis, or additional PR links to support claims
- **Two-phase approach works well**: First get thematic analysis of the JSON, then write your own narrative and ask the LLM to "add relevant PR links as evidence for each technical claim"
- **If output feels wrong**: Check if it's leading with blockers instead of accomplishments, dwelling on resource drama, or making unverifiable claims. Ask for a rewrite that orients around what shipped.

