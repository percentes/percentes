# SPEC-HOSTED amendment drafts

HA1 and HA2 are hosted amendments 1 and 2. HA1 is adopted as modified by
HA3 (SPEC-HOSTED.md amendment log, 2026-10-11); HA2 is drafted and not
adopted. A draft is not normative until it is adopted with a date in the
SPEC-HOSTED.md amendment log. Section numbers below are SPEC-HOSTED.md's
unless prefixed SPEC.md.

# HA1: the load axis

Drafted 24 August 2026, revised 26 September 2026. It replaces the single
pinned rate and the headroom rule of §3 with a ladder of rates and a
ceiling rule. You need this text when reading SPEC-HOSTED.md §0 point 5,
which adopts it with the changes listed there; a release runs under the
adopted form, and the text below is the source it modifies.

Adopted 2026-10-11 as modified by HA3 point 5 (SPEC-HOSTED.md §0), which
governs where the two differ: the drain, the backoff and G2 evaluation at
each drain's end, the ceiling rule and the idle pool.

## §3 replacement: the offered-load ladder

**Ladder.** Offered load is a ladder of pinned steps, open-loop Poisson
arrivals at each step: 0.25, 0.5, 1, 2 and 4 requests per second, run in
ascending order, each step an 8-minute measured window, after the §3 60 s
warm-up run once at the first step. One ladder is one cell (§4).

**Ceiling rule, replacing headroom.** Before a ladder runs, every numeric
limit the provider publishes for the (provider, account, model) triple is
recorded with URL (Uniform Resource Locator) and retrieval date: request
ceilings per minute and per day, token ceilings per minute and per day, concurrency caps, and spend
caps with the per-token prices they are computed from. A step's nominal
demand is its rate times the pinned request shape over the window a
ceiling states, token demand doubled as the allowance for the provider's
tokenizer (§6), and spend at the recorded prices. A step is excluded
before the run where its demand over 60 s reaches a recorded per-minute
ceiling, where its rate times the pinned 30 s timeout reaches a recorded
concurrency cap, or where its demand over 8 minutes, added to every lower
step and to every other ladder scheduled for the triple on the same day,
reaches a recorded per-day or spend ceiling, the day being the provider's
documented quota window or, where none is documented, the UTC
(Coordinated Universal Time) day. The exclusion publishes alongside the
results with the binding ceiling named. A provider publishing no ceilings has no excluded
steps for that reason, and the absence is published. The §3 balance and
quota records stay as they are.

**Backoff rule.** A step whose status_429 fraction (§5, the SPEC.md §3
class status_429) over the step's scheduled requests exceeds 0.5
terminates the ladder: higher steps are recorded as not attempted with the
terminating step named. Statuses the provider documents as overload stay
in status_other (§5) and do not count toward the fraction. The instrument
never retries and never resumes above a terminated step within the cell.

**What publishes per step.** The §5 outcome split (completed, errored with
class, censored), the status_429 fraction as a headline, the
completion-incidence curve, and the conditional latency quantiles, each
labelled with the step's offered rate. "Under load" in any published
sentence refers to this ladder and names the highest attempted step.

**Client validity.** The SPEC.md §2 client-validity gate, G2 in §7,
applies per step. A step failing it is withheld and the ladder stops
there; a ladder stopped by a G2 failure does not count toward frame
completeness, and its cell is re-run under §4 from the lowest step.

## Consequential edits on adoption

- §0: "under a fixed open-loop load" becomes "at each step of the pinned
  ladder". §1: the question is asked at each ladder step.
- §3: the rate, duration (60 s warm-up, then five 8-minute measured
  windows), expected-sample-budget (rate times 480 s per step, 120 to
  1,920 scheduled requests) and nominal-demand (per step) bullets are
  restated; in the transport bullet, `load.connections` 32 is added to
  the provision derived from the top step, two times 4 requests per second
  times 30 s, so an idle pool of 272; the headroom paragraph is superseded
  by the ceiling rule above.
- §4: a cell is one ladder; the expected request count is 3,720 scheduled
  requests per cell and 44,640 per characterization.
- §5: the status_429 headline publishes per step, and its paragraph's
  references to the headroom rule and to half the published rate are
  restated against the ceiling rule.
- §6: the ladder's rates and step duration join the pins this document
  adds, and "the headroom rule" in that list becomes "the ceiling rule".
- §7: G2 applies per step as above.
- §8: every per-run figure publishes per step, labelled with the rate, and
  pooled figures pool each step separately across the 12 cells.
- §9: the required hosted profile carries the ladder steps and one
  measured window per step in place of the single §3 rate, with the
  connection provision derived from the top step; the required instrument
  changes gain the stepped schedule with per-step windows, the pre-run
  ceiling exclusion, the termination rule, and per-step G2 evaluation and
  reporting.

# HA2: the descriptive pilot

Drafted 26 September 2026; not adopted. It authorizes one kind of run the
protocol as written does not: a paired, task-scored, descriptive comparison of two
endpoints for one named reader, reported privately. It changes nothing for
a characterization under §1 to §9; a pilot run is labelled as one and
enters no characterization. You need this text only to read a pilot
report or the `cmd/percentes-pilot` command that runs one; nothing in a
release depends on it.

## What a pilot is

A pilot is two endpoints (provider, model identifier, account tier each),
one task, six paired sessions on distinct dates, one client host, and one
report to the person who named the pair. Its rows describe those sessions.
It publishes no winner, no grade, no interval claim, no 99th percentile,
and no figure across pairs other than the mean and range of the six
per-pair differences. A tied or inconsistent table is an admissible
result.

**Terms.** The measured cases are 120 frozen task inputs with a frozen
label each. The development cases are 24 further inputs, disjoint from the
measured cases, used to build and check the prompt, the rubric and the
scorer before freezing; they are never sent to either endpoint in a
session. The scoring cutoff is 10 s from intended dispatch, inclusive; it
is a scoring threshold and leaves the SPEC.md §3 deadline, the pinned 30 s
timeout from actual dispatch, unchanged. The pre-run probe is §5's.

**Roles, recorded in the manifest.** Who chose the task, who supplied the
cases and labels, who wrote the rubric, and the interest of the person who
named the pair. The task, the cases, the labels, the rubric and the scorer
are frozen by hash before either endpoint is called.

## Replacements, section by section

**§1 target and comparison scope.** For a pilot, the complete request
path is configured per endpoint and recorded, and the harness appends
nothing; the §1 sentence on path shape does not apply. A two-endpoint
descriptive table is permitted: per pair, each endpoint's counts side by
side and their difference; across pairs, the mean and range of the six
differences. The SPEC.md §3 bar on comparison tables without interval
bands stands for any ranked claim and any interval; a pilot table ranks
nothing and carries no interval, and SPEC.md gains the sentence below on
adoption.

**§2 endpoint.** Both endpoints receive the same literal system text, user
text, requested output schema and generation settings, pinned by hash;
settings one provider does not support are recorded as not applied for
that endpoint. Provider-native schema enforcement is either applied to
both or its absence on one is recorded as part of what is compared.
Reasoning settings are pinned where a provider exposes them.

**§3 load and admission.** Deterministic open-loop arrivals, one request
every 10 s: six warm-up requests over one minute, each carrying the first
measured case of the session's permutation, discarded, then 120 measured
requests over 20 minutes per endpoint per session. A slow
response never delays a later dispatch. The input is the task prompt for
one measured case, without a unique prefix: the cases repeat across
sessions, the report says so, and cache telemetry the provider exposes is
recorded (missing telemetry is recorded as unknown). Output ceiling 4,096
tokens. The pilot's input ceiling is the largest prompt token count the
provider reports, with usage requested, for the 120 measured cases, each
sent once to each endpoint before the first session and recorded in the
manifest. Nominal demand is six requests per minute and six times the sum
of the input ceiling and 4,096 completion tokens per minute; at a
2,000-token input ceiling, for example, it is 36,576 tokens per minute. A
session runs only where its nominal demand, and its totals against any
daily or spend window at the recorded prices, are at most half of every
limit the provider publishes for the tier, recorded with URL and retrieval
date; the provider's own quota windows and token reservation rules are
applied as published, and a limit the provider does not publish is
recorded as absent and imposes no test. Two local tripwires record, and never block or delay, a
dispatch: a request beyond the scheduled count plus warm-up, and a fifth
concurrent in-flight request. Either one makes the window client-invalid.

**§4 frame.** Six sessions on distinct dates, three weekdays and three
weekend days, one slot at 20:00 UTC, dates and the per-session case
permutation frozen before collection. Within a
session the two endpoints run consecutively, the second starting within
five minutes of the first draining; which runs first is drawn and frozen
before collection, three sessions each way. One reserve date of each
day type is frozen with the schedule: at most one replacement session per
pilot, on the reserve date of the replaced session's day type, for a
session whose window was client-invalid, with the replaced session's
endpoint order, both attempts retained. Fewer than six
compatible, client-valid pairs after that replacement labels the pilot
incomplete, with the actual pair count and every unmatched window shown.
No adverse endpoint result is re-run.

**§5 outcomes.** The transport classes apply unchanged. Task outcomes are
recorded beside them per request: correct and complete by the scoring
cutoff; correct after the cutoff; wrong; refusal; length limit; schema
failure (extra prose, extra or duplicate keys, wrong type, unknown value);
instrument-incompatible (a response contract the client does not
understand). After a successful pre-run probe, a 429, a service error or a
timeout is a result of the session and is never excluded for performing
poorly; a window is client-invalid only on independent evidence: a missed
dispatch, a process failure, a wrong recorded configuration, a tripwire,
or a failed G2 gate. An instrument-incompatible response blocks the
affected task comparison until the client understands the contract; the
attempt is retained as unscorable and is never a provider row.

**§6 pins.** As §6, except the output ceiling, the fixed input length and
the unique per-request prefixes, which §3 above replaces; plus the
measured and development cases and their labels by hash, the warm-up
request body, the rubric, the scorer, the scoring cutoff, the case
permutation per session, the endpoint order per session, the generation
settings per endpoint, and the requested and reported model identifiers
per request. A change in reported identity, or in whether the provider
applies a pinned generation setting, inside the series splits the series
at the first session where it was observed: each segment reports its own
pairs, the mean and range of differences are computed within a segment,
and a pilot with no segment of six pairs is labelled incomplete with the
per-segment pair counts shown.

**§7 validity.** G2 applies per window, run-failing for that window.
Hosted-G6 is reported per window. The headline is correct, complete
answers by the scoring cutoff over all scheduled measured requests,
numerator and denominator shown.

**§8 reporting.** Per pair: the headline for each endpoint and the
difference; task correctness by the 30 s deadline; schema failures,
refusals and length limits; the status and error classes; time to first
visible content and completed-answer latency as distributions labelled
client-observed and conditional on their cohort, with excluded outcomes
beside them; billed usage where the response carries it; chunk counts
labelled as chunks. Across pairs: the mean and range of the six
differences. With the table: the class counts of the frozen labels and, per
endpoint per pair, the matrix of the scored answer label against the
frozen label over that pair's measured requests, so a majority-class
answer is visible.

**§9 artifacts.** As §9, plus a manifest carrying the hashes and roles
above, the permutations and orders, the pre-run probe results, and the
account quotas as read; one JSON Lines (JavaScript Object Notation, one
object per line) record per attempt with timed raw frames retained; the scorer's output per attempt; and a replay that
reproduces every reported count from the artifacts. The report goes to the
person who named the pair and is published only with that person's
agreement, with nothing about their organization in it.

## Required instrument changes for a pilot

Beyond §9's list: a standalone hosted command with no replica count, no
fault instant, no share gate and no SPEC.md §4 verdict, carrying a pilot
configuration of deterministic arrivals at 0.1 requests per second, a
60 s warm-up and a 1,200 s measured window, with the output ceiling and
the prompt taken from the literal request body in place of the pinned
max_tokens and unique-prefix rules; the configurable complete endpoint
and literal request body; a parser that keeps the finish reason, usage,
model identity and in-band errors and classifies the termination shapes
above; the scorer and its fixtures (boundary, wrong-label, duplicate-key,
refusal, length-limit and partial-answer cases with frozen expected
counts); the tripwires; and the JSON Lines export with replay.

## Consequential edits to SPEC.md on adoption

- §3, after the interval-band sentence: "A descriptive pilot table under
  SPEC-HOSTED.md amendment HA2, two endpoints, per-pair counts and their
  differences with the mean and range of those differences, no ranking and
  no interval, is not the comparison this sentence bars."
- §6, in the hosted-target list: "Under a SPEC-HOSTED.md HA2 pilot the
  output ceiling, the fixed input length and the unique per-request
  prefixes are replaced by that amendment's §3."
- CHANGELOG.md, dated with the adoption: "§3, §6: a descriptive pilot
  under SPEC-HOSTED HA2 (two endpoints, per-pair counts and differences
  with their mean and range, no ranking and no interval) is not a barred
  comparison; its output
  ceiling, input length and prompt repetition follow the amendment."

Adoption line for the SPEC-HOSTED.md amendment log: "HA2 (2026-__-__,
pre-data): descriptive pilot authorized; pilot runs enter no
characterization."
