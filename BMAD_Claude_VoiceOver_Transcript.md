# BMAD with Claude — Voice-Over Transcript

**Video length:** ~6 min 4 sec
**Pacing:** ~150 words per minute (comfortable AI TTS rate)
**Focus:** Using BMAD with Claude to produce *project definition and requirements* — not the project itself.

---

## [0:00 – 0:20] Opening — set the frame

What you're about to see isn't a coding demo. It's a planning demo. The thing most engineering teams get wrong on AI projects isn't the code — it's the definition. The fuzzy brief, the missing risks, the requirements that surface only in sprint three. In this walkthrough, I'll show how BMAD, running inside Claude Code, turns that fuzzy front end into a structured, review-ready set of artifacts — before a single line of code is written.

---

## [0:20 – 0:45] What BMAD is, in one breath

BMAD is a method, packaged as a set of skills Claude can invoke. Think of it as a guided workflow for the discovery and definition phase of any product. On screen, you can see six modules already loaded — the BMAD Method, Core, Builder, Creative Intelligence Suite, Game Dev Studio, and Test Architecture Enterprise. The output and docs folders are empty. Nothing has been built yet. That's intentional. We're starting at zero, and we're going to define a product end-to-end.

---

## [0:45 – 1:15] The first move — Product Brief, not code

The first command is `bmad-product-brief`. Notice what Claude does here. It doesn't ask me to write code, scaffold a repo, or pick a framework. It asks me what the product *is*. The example I'm using is a Fraud Research Engine — a multi-agent platform with a natural-language query interface for healthcare claims investigators. That's a meaty problem. And BMAD treats it like one. It pulls the idea into a structured brief covering architecture, the query layer, the domain areas, anomaly detection, and reporting.

---

## [1:15 – 1:50] Multi-agent review — the part most people skip

Here's the part I want you to pay attention to. Once the draft brief is written, BMAD doesn't hand it back to me. It launches a review panel — in parallel. A Skeptic agent challenges the assumptions. An Opportunity agent looks for upside I missed. And a Regulatory and Compliance reviewer pressure-tests the riskiest dimension for a healthcare AI product. You're seeing real findings appear — gaps in the LLM hosting model, missing Business Associate Agreement language, model governance and version control concerns. This is the kind of review you'd normally pay a consulting firm to run.

---

## [1:50 – 2:20] The brief sharpens itself

Watch the diff on screen. The original brief said the product helps investigators. The revised brief now says it saves forty to sixty percent of investigation time across roughly two thousand SIU analysts at UHG — equivalent to adding eight hundred to twelve hundred virtual analysts. That's not a tone change. That's the difference between a brief a CFO will fund and a brief that gets sent back. BMAD is teaching the brief to speak the language of P and L impact.

---

## [2:20 – 2:50] Brief plus distillate — feeding the next phase

When the brief is complete, BMAD produces two artifacts. An executive brief — the human-readable version — and a distillate, which is a token-efficient context pack for the next workflow stage. This matters more than it sounds. Every downstream skill — the PRD, the architecture, the epics — consumes the distillate. So context is preserved, nothing gets lost, and we don't burn tokens re-explaining the product to Claude every time we open a fresh window.

---

## [2:50 – 3:20] Moving to the PRD

Next command — `bmad-create-prd`. Claude classifies the project automatically. SaaS B2B. Healthcare and insuretech. High complexity. Greenfield. That classification drives everything that follows — which sections are required, which are skipped, which questions get asked. A SaaS B2B PRD looks nothing like a mobile-first PRD, and BMAD knows that. It sharpens the vision statement, locks in the differentiator, and starts asking the questions a senior product manager would ask.

---

## [3:20 – 3:50] Success criteria that survive contact with reality

Look at the success criteria it generates. Three-month pilot targets and twelve-month scaled targets, side by side. Analyst adoption, investigation time reduction, fraud cases identified, false positive rates, workforce multiplier — each with measurable numbers. Then technical success criteria — query response times, ninety-nine point five percent uptime, zero PHI exposure incidents, full auditability, hallucination rates under five percent. This is what a real PRD looks like. Most teams don't get here until month four.

---

## [3:50 – 4:25] User journeys — written like product fiction

BMAD now writes the user journeys. Not as bullet points — as narratives. Maria the analyst on the success path. Maria on an edge case. David the compliance lead during quarterly audit prep. Raj the system administrator handling a latency alert. Priya the API consumer integrating with downstream systems. Each journey has an opening scene, rising action, climax, and resolution. It reads like product fiction. And out of those four journeys, BMAD extracts a clean capabilities matrix — exactly what the engineering team needs to build.

---

## [4:25 – 4:55] Domain requirements — the healthcare reality

This is the section that separates a serious PRD from a pitch deck. Domain-specific requirements. HIPAA and PHI protection. FDA considerations. Clinical validation. Insurance regulations. State-level data retention. Then the risk register — false accusations from AI findings, PHI breach via the natural language interface, model bias in anomaly detection, regulatory rejection of AI evidence — each with a specific mitigation. Conservative confidence thresholds. Mandatory human review. Daubert-ready evidence chains. This is the language regulators speak.

---

## [4:55 – 5:25] Innovation patterns — what's actually new

BMAD then flags the genuinely novel parts of the product. Multi-agent investigative orchestration as a category-defining pattern. The shift from detection and alerting to investigative intelligence. And critically, it pairs every innovation with a fallback — what happens if multi-agent orchestration produces contradictory reports, what happens if the natural language interface fails, what happens if cross-domain correlation generates false connections. Every novel idea ships with a safety net. That's how you take innovation through a healthcare review board.

---

## [5:25 – 5:50] Why this matters

So step back and ask — what just happened. In the time it would have taken a small team to schedule a kickoff meeting, Claude with BMAD produced a structured brief, a distillate, a classified PRD, success criteria with measurable targets, four user journeys, a domain requirements pack, a risk register with mitigations, and an innovation map. All reviewable. All editable. All version-controlled as markdown.

---

## [5:50 – 6:04] Close

This is what I mean when I say the leverage is in definition, not generation. BMAD with Claude doesn't replace the product manager, the architect, or the compliance lead. It gives them a head start measured in weeks. And in regulated domains, that head start is the difference between shipping and stalling.

---

## Pacing notes for Clipchamp

- Each section header gives you the target time window — paste the corresponding paragraph into your TTS tool and trim if your chosen voice runs faster or slower than 150 wpm.
- Natural breath points are the em-dashes and the period after the section's last sentence — let the AI voice land on them.
- If a section runs long against the on-screen action, the safest cuts are the example numbers (e.g., the "two thousand SIU analysts" line in section 5) — they're illustrative, not load-bearing.
- If a section runs short, hold silence over the on-screen scrolling — the visual carries it.
