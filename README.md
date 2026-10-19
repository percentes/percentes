# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes is a measurement instrument for large language model (LLM)
inference services. It sends requests on a schedule fixed in advance,
records what happened to every one of them, and reports the result with
the evidence a reader needs to recompute it. It serves two measurements:

- **A self-hosted service under failure.** [SPEC.md](SPEC.md) §1 asks
  what a replica loss costs a Kubernetes-served vLLM (an open-source LLM
  serving engine) deployment: the requests the loss kills or strands, the
  degradation the surviving replica takes on, and how long recovery
  takes, split into measured sub-phases.
- **A provider's hosted application programming interface (API) under
  load.** [SPEC-HOSTED.md](SPEC-HOSTED.md) measures a managed endpoint
  from one client host through one ordinary paid account: how many
  scheduled requests complete in time, how many fail and how, at up to
  five offered-load steps, each admitted only where its nominal demand
  (the step's request rate, and its tokens and spend at the worst case
  per request) stays within half of every rate, token, concurrency,
  daily and rolling spend limit recorded for the account
  ([Measuring a hosted endpoint](#measuring-a-hosted-endpoint)).

You need the first if you run an inference service and want to know
what one lost replica does to live traffic. You need the second if you
buy LLM API capacity and set retries, fallbacks and timeouts from how
the provider fails.

Three rules shape every measurement:

- **Open loop.** Every request's send time is fixed before the run, and
  the generator never waits for an answer before the next send. If it
  waited for slow answers, it would send fewer requests exactly while
  the service is slow and under-count that period, an error known as
  coordinated omission. Every latency is measured from the request's
  *intended* send time.
- **Three-state outcomes.** Every dispatched request ends in exactly one
  of completed, errored or censored (observed to the client timeout with
  no terminal event: the pinned 30 s, or the hosted release's 90 s hard
  timeout). A scheduled request a stop left unsent is reported on its own
  line. Only completions enter latency histograms. Failure rates are
  reported as figures in their own right. The completion curve gives,
  for each time t after the intended send, the share of all scheduled
  requests completed by t, estimated with the Aalen-Johansen cumulative
  incidence estimator. Errors are competing terminal events on the curve.
  A timeout is censored at the moment it timed out, and under the release
  profile a scheduled request a stop left unsent is censored at the
  curve's horizon, the end of the observed time (SPEC §3, SPEC-HOSTED
  §5).
  - A latency quantile the completion curve never reaches inside the
    horizon is refused and is never extrapolated. Add the share of
    scheduled requests completed when the curve ends to the share still
    without an ending (censored). Where that sum reaches the quantile,
    the report prints the quantile as greater than the horizon. Where it
    does not, the errored requests alone hold the curve below the
    quantile, and the report prints it as unattainable.
- **Run-invalidating self-checks.** The client must show it was not the
  bottleneck: send-skew, undispatched-request, central processing unit
  (CPU) and garbage-collection (GC) pause gates, all pinned and
  run-failing. These are the client gates; the full run-validity set is
  SPEC §10. A run that cannot show its own validity exits invalid
  instead of publishing.

| Document | What it is | Read it when |
|---|---|---|
| [SPEC.md](SPEC.md) | the authoritative, pre-registered specification of the replica-loss experiment: every gate, tolerance and detector parameter carries a pinned number, enforced when a configuration loads, so a configuration that weakens one refuses to load | you want to know what a self-hosted run measures, or why a configuration refused to load |
| [CHANGELOG.md](CHANGELOG.md) | every change to those rules since first publication, dated | you want to know what changed and when |
| [SPEC-HOSTED.md](SPEC-HOSTED.md) | the hosted protocol SPEC.md §6 requires before any provider data is collected; its body carries the adopted amendments, its amendment log and Appendix A record them, and it opens with a plain summary for a first-time reader | you want to know what a hosted figure means |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | how the code implements both documents: the packages, the commands, the evidence files, and one request followed from its schedule to its line in the report, with diagrams | you want to change the code or check a published number against it |

## Scope

Percentes is built in phases. Phase 0 builds and certifies the
measurement instrument against a mock OpenAI-compatible server-sent
events (SSE) server on a local kind cluster. Passing the
acceptance-criteria (AC) suite certifies the measurement machinery only;
real graphics processing unit (GPU) claims wait for Phase 1, which
characterizes real vLLM on GPU hardware with the same harness and gates.
Phase 1's SPEC §10 calibration procedure ran on 16 September 2026 on one
NVIDIA L40 GPU against a standalone container; the calibration inside
the experiment's Kubernetes environment and the characterization runs
have not yet run.

## Quickstart (no GPU required)

This section gets the tree built and tested on one machine. Requires Go
1.21 or newer, Docker, [kind](https://kind.sigs.k8s.io/), kubectl,
python3 and curl. The Makefile and the git hooks select Go 1.21.6, which
is the toolchain the pinned linter can read; set `GOTOOLCHAIN` yourself
to override.

```
make hooks                    # once per clone: continuous-integration (CI) gates on commit and push
make test-unit                # fast path: unit and integration tests only
make reproduce                # AC7: one-command full harness run against the local cluster
make test                     # the whole gate: unit + AC suite + kind smoke + reproduce + campaign e2e
make percentes-pilot          # bin/percentes-pilot, the hosted pilot command
make percentes-release        # bin/percentes-release, the release runner for this host
make percentes-release-linux  # a static linux/amd64 build of it for the client host
```

In that order: `make hooks` before your first commit, `make test-unit` to see
the tree is sound, then the cluster targets. `make test` runs all of them.
The two hosted binaries are needed only for the hosted measurement.

`make hooks` points git at `hooks/`: gofmt, build and golangci-lint on
commit (`go vet` when the linter is absent), and the race unit suite on
push, so a change fails locally before it fails CI. `hooks/commit-msg`
requires a subject line of 55 characters or fewer in lowercase
conventional style, with no body.

The full gate's last three stages build a Docker image and drive a kind
cluster. The AC suite measures this machine as well
as the code: before its timing-coupled tests it qualifies the host with
a probe (timer wake lateness and garbage-collection pauses,
`internal/hostqual`). On a host that fails the probe those tests skip and
`make test-ac` fails; `PERCENTES_AC_ALLOW_SKIPS=1` accepts the skips. When
a timing-coupled assertion fails, the host is probed again: a host that
lost qualification during the run skips the test, and one that kept it
fails.
The cluster stages bind host ports 18080 to 18082, which `SVC_PORT`,
`POD_PORT` and `ADMIN_PORT` override, and host port 18000, the kind
mapping to the mock Service's NodePort 30800, which is fixed in
`deploy/kind/kind-config.yaml`. On macOS the host CPU gate reports
unmeasured, so every cluster run prints `RUN INVALID (run-failing gate)`
and exits 2 while the stage itself passes: the gates are enforced, and
this platform cannot supply one of their inputs. Reports are written to
`results/`.

## Layout

```
cmd/percentes            single-run harness command-line interface (CLI) for a self-hosted target; replay and the completeness check of a release cell
cmd/percentes-campaign   N-run campaign runner for a self-hosted target (SPEC §5 repetition, §10 gates)
cmd/percentes-calibrate  SPEC §10 capacity ramp and §5 single-replica reference
cmd/percentes-pilot      the SPEC-HOSTED hosted amendment 2 (HA2) pilot: ledger, probe, dev, session, check-cpu
cmd/percentes-release    a SPEC-HOSTED hosted amendment 3 (HA3) release: key, schedule, ledger, ceiling-check, run, dispatch, pages, publish-check, rehearse, check-cpu
cmd/mockserver           fault-injectable mock inference server
cmd/naivesweep           standalone reconnaissance probe, outside the instrument
internal/                loadgen, collect, detect, orchestrator, validity, ...
                         hosted: probe, admission, evidence, pilot, score, schedule, releasekey
configs/                 pinned configurations; a *.reference.yaml per profile
deploy/                  kind, mock and Phase 1 manifests; reproduce and capture scripts
docs/                    ARCHITECTURE.md and the draw.io diagrams under docs/diagrams/
```

## Measuring a hosted endpoint

This section is for an engineer who wants to measure a provider's API
with the instrument, or to read a published hosted result. Every hosted
run writes the evidence its figures are recomputed from, and a release
runs from first command to published page through the commands listed
below.

### What every hosted run writes

A hosted target (`target.hosted: true`) is a provider's managed endpoint.
SPEC-HOSTED.md pins what a hosted run sends and records. Two
configuration profiles carry those pins. `release` carries the adopted
protocol (hosted amendments 1 and 3, HA1 and HA3, written into its
body) and is the path to published hosted figures. `pilot` carries the
numbers of a drafted amendment, HA2, that is not adopted, and reports
privately. Each profile refuses a value outside its pins. Hosted runs
go through `percentes-pilot` or `percentes-release`, which reserve every
request in the admission ledger before it is sent; `percentes` and
`percentes-campaign` refuse a hosted target. A run's report pair is `report.json`, the
machine-readable report, and `report.txt`, its human-readable rendering,
from one configuration. Every hosted run writes the evidence
SPEC-HOSTED §9 requires:

- `attempts.jsonl`, one record per scheduled request;
- the raw response frames, in a store outside the source tree;
- the client clock offset, and the endpoint status at start and end;
- the pre-run probe's record;
- a manifest that hashes them all with the configuration, the operator's
  documentation records and the admission ledger's position.

A release cell adds four more records: the refresh record (the
cell-start run of the command that re-reads the account's limits and
model documentation pages), the balance records of its start and end,
the ceiling-check records of every route that serves the same model, and
its dispatch series (sends per second of Coordinated Universal Time,
UTC). Once no cell is left, the route's process writes the route's
window-close record, `window-close.json`: the limits and model
documentation read once more and compared with the last counted cell; a
stopped route's process writes it when it next runs. The report pair is
rendered from those files alone. Anyone holding a cell's published files
can run `percentes --replay DIR --config FILE
--out OUT` (OUT a directory other than DIR): it renders the report pair
again from `manifest.json`, `attempts.jsonl` and the configuration file,
and exits 1 unless both files match the hashes the manifest records,
byte for byte. A route with a confidential limit (a value only the
account console shows, under terms that keep it confidential) publishes
its configuration with the quoted string `"[retained]"` in place of that
value, and that file does not load, so its cells replay only from the
retained file as loaded. `percentes --check-release DIR --config FILE
--frames ROOT` and `percentes-release pages` also read the frame store,
which holds the model's answers where the route's terms permit keeping
them and stays unpublished, so only the operator can run them. A pooled
count on a route page is the sum, over the cells that attempted the
step, of the cell's non-warm-up records in its published
`attempts.jsonl` with that `step` and outcome; its interval follows from
those counts by the formula in SPEC-HOSTED §8.

### The pilot

`percentes-pilot` runs the descriptive pilot drafted as SPEC-HOSTED
amendment HA2 (hosted amendment 2) in SPEC-HOSTED-AMENDMENT-DRAFT.md. HA2
is not adopted. A pilot is two endpoints, one task and six paired
sessions on dates derived from a master seed, reported privately to one
named reader; it enters no release, and nothing in a release
depends on it. Keys come from a file given with `--env` (`NAME=value`,
readable by its owner only) and are never printed or recorded; every
request is reserved in the admission ledger before it is sent. The
command's package documentation lists its subcommands and exit codes:

```
go doc ./cmd/percentes-pilot
```

### A release

`percentes-release` runs a public descriptive release under SPEC-HOSTED
amendment HA3. A route is one provider, one model, one endpoint and one
account tier; each route has one configuration file, which carries the
complete request uniform resource locator (URL), the whole request body
as literal JavaScript Object Notation (JSON) frozen by hash, the header
the key travels in, and every limit recorded for the account with its
source and a marking: `public` where a public documentation page gives
the value, which then publishes in full, or `confidential` where only
the account console shows it under terms that keep it confidential.
A confidential limit's value is absent from the configuration as
published, the ceiling rule's verdict, the refresh records and the
ceiling-check snapshots. The configuration and the verdict name such a
limit by its source and the keyed hash of its retained copy. A keyed
hash here is an HMAC-SHA256 (a hash-based message authentication code
built on SHA-256, the 256-bit Secure Hash Algorithm) under the release
key: 32 random bytes that `percentes-release key create` writes once,
at the freeze, to a file only its owner can read. Nobody without the
key can check a guess against a keyed hash. Keep the key file
private, as the API keys are: copy it to the client host, and leave it
out of the repository.
A provider can echo a limit in its rate-limit headers or in the body of
its limits page, so a route with a confidential limit must withhold
both: `target.retained_headers` must cover every recorded response
header other than `Retry-After` and a request identifier such as
`X-Request-Id`, and `target.probe.limits_retained` must be set where
the route reads a limits URL. On such a route the probe and
ceiling-check records also carry each non-200 response body as its
keyed hash, and those records and the attempts file carry each in-band
error object as a stand-in with its keyed hash. The texts stay in the
retained store. Each cell's manifest names the frame store and the
operator records by keyed hash, with no byte count, and the route's
window-close record names the operator records the same way; the
manifest names its other files by SHA-256 hash and size. The routes of
a release run as separate processes on one client host, each at its own
offset inside the slot.

| Setting | Value |
|---|---|
| Ladder | five steps at 0.25, 0.5, 1, 2 and 4 requests per second, Poisson arrivals (random and independent), 480 s each, with a 90 s drain after every step in which nothing is sent, after a 60 s warm-up |
| Cell | one run of the ladder: 48 minutes 30 seconds with all five steps admitted |
| Frame | twelve cells per route: six UTC start slots (00:00, 04:00, 08:00, 12:00, 16:00 and 20:00), each run once on a weekday and once on a weekend day, inside a committed window of at most 14 days |
| Deadline and hard timeout | 30 s from intended dispatch splits completions into in time and late; 90 s from actual dispatch censors a request with no terminal event |
| Request shape | Shape S: one frozen body per route with a synthetic support ticket per request, under a 1,200-token input bound and a 512- or 1,024-token output ceiling |
| Admission | a step runs only where its nominal demand is at most half of every recorded rate, token, concurrency, daily and spend-window limit, and only where the account's monthly cap and the route's liability cap cover twelve cells, two re-runs and 100 probe requests |
| Before each cell | the route's refresh command rewrites the recorded limits and model documentation and prints the limit values it read, and its balance command reads the account's balance; a cell whose records were not rewritten, or whose balance is below the cell's reservation, is blocked before any request, and a route whose limits as read, at its newest recorded price, no longer admit one of its steps stops |
| After the last cell | the refresh command runs once more; its record is the route's window-close record, compared with the last counted cell and printed on the route's page |
| Model identifier | each response's reported model is compared with the expected one (the requested identifier, or the one `target.reported_model` records from the model documentation); a cell with any other is labelled `model_mismatch`, kept out of the frame and re-run, and the route's page lists it with the expected identifier, every identifier reported and how many requests reported another |
| Retries | none; `Retry-After` is never honoured, and a release configuration must record it under `target.response_headers` |
| Retention | the frame store keeps the model's answers, unpublished, for 12 months, and only where the route's recorded terms permit it (`target.retention`); otherwise it counts their bytes and keeps none |

Every finished cell writes a labelled manifest and report pair; a cell
that does not count is kept as a record outside the route's frame.

| Command | When | What it does |
|---|---|---|
| `percentes-release key create`, `percentes-release key sum` | before the window, before `ledger create` | `key create` writes the release key to a new file readable by its owner only, and refuses an existing file; `key sum` prints each named file's keyed hash, the `hmac_sha256` a confidential limit records for its retained copy; `schedule` and `run` read the key from `--release-key`, as `ceiling-check`, `pages`, `publish-check` and `percentes --check-release` do where a route has a confidential limit |
| `percentes-release ledger create` | before the window | starts the admission ledger the routes share, with every route's liability cap |
| `percentes-release ledger price` | before the window | records a route's list prices at the token bounds of its cells and of its ceiling check; the ledger admits a request only at a bound it holds a price for |
| `percentes-release ceiling-check` | before the window | sends a route's ten-request ceiling check, admitted in the ledger, and writes the record `run` reads, which names that ledger by the SHA-256 hash of its first line; it needs `--frames` where the route withholds probe values, and `--release-key` too on a route with a confidential limit; exits 2 when the check fails or is blocked, or when the record reports a model identifier on which `run` refuses the route; `run` needs the record of every host of the route's model, binds each to the request it checked, refuses one whose ledger hash is not the one the schedule froze or that carries a value its host's configuration withholds (`run` reads each other host's configuration, as the schedule froze it, from `--host-config`), re-derives its results from its requests, refuses the route when its own records report a model identifier other than the expected one, and leaves out of the model's ceiling choice another host whose records report an identifier other than the one they expect |
| `percentes-release schedule` | before the window, after the ceiling checks | writes the committed schedule of every route from one master seed and the window's dates; it freezes each route's configuration as published and the ledger by their SHA-256 hashes, the configuration as loaded by its keyed hash, and each route's host group; `run` refuses any other at the route's start, and checks the ledger's first line again at each cell; it never replaces an existing file |
| `percentes-release public-config` | at the freeze | writes a route's configuration as published, each confidential limit's value replaced by the quoted string `"[retained]"`; the freeze commits that file, and every other command reads the file as loaded |
| `percentes-release run` | from the first scheduled slot, one process per route | runs the route's cells over the window and writes each cell's evidence, labelled manifest and report pair; at each cell's start it runs the refresh and balance commands, blocks the cell where either does not let it start, and stops the route where the limits read no longer admit a step; once no cell is left it writes the window-close record |
| `percentes-release dispatch` | after the window | sums every cell's dispatch series into the host's requests per second |
| `percentes-release pages` | after the window | writes the index page and each route's page from the cells' records and each route's window-close record; a route's page lists each `model_mismatch` cell with its identifiers; it refuses cells of one model that carry different ceiling choices or ceiling-check records; a route whose cells hold only missed or interrupted attempts with no manifest gets no page, and its configuration's ceiling must equal the ceiling of its model's other hosts |
| `percentes-release publish-check` | before publication | checks the release publication record (each provider's notice, its window of at least four working days, its response and consent, the naming permission and replication pack where a route needs them) against the cells and the pages, and exits 2 while anything is missing |
| `percentes-release rehearse` | before the window, on the client host | the acceptance run: each of six mock routes, which the command serves itself on loopback, gets its ten-request ceiling check and then one cell, run at their offsets up the whole ladder; writes `rehearsal.json`, which says whether the rehearsal is green (among [its checks](docs/ARCHITECTURE.md#the-rehearsal), every cell counted with G2, the check that the client machine was not the bottleneck, passing at every step); at full length, the default, that file is the run's evidence |
| `percentes --replay`, `percentes --check-release` | any time after a cell | render a cell's report pair again and compare it byte for byte; refuse a cell whose files, the unpublished frame store among them, are missing or differ from their recorded hashes |

```
go doc ./cmd/percentes-release
```

### One request, end to end

The steps below follow one request of a release from the schedule to its
line in the published tables.

1. **Schedule.** The committed schedule gives the route a cell (a date
   and a UTC slot) and the route's offset inside the slot. The cell's
   seed fixes the request's step and its intended send time.
2. **Admission.** At the cell's start, after the clock and status
   records, the refresh command rewrites the recorded limits and model
   documentation and the balance command reads the account's balance;
   a cell whose records were not rewritten, or whose balance is below
   its reservation, is blocked here, and a route whose limits as read,
   at its newest recorded price, no longer admit one of its steps stops
   here. The ledger then reserves one probe request plus every scheduled
   request of the cell against the route's and the release's liability
   caps; a refusal stops the route before any request is sent. The
   request's step was admitted by the ceiling rule when the route
   started.
3. **Body.** The request's user text is the prefix `cs-<seed>-<index>`
   (six-digit index), a space and a synthetic support ticket. It replaces
   the one slot in the route's frozen body; every other byte is sent as
   configured, and the key travels in the one header the configuration
   names.
4. **Send.** The client sends at the intended time and records the send.
   The 90 s hard timeout runs from the send.
5. **Stream.** The first content event stamps the time to first token.
   `data: [DONE]` ends a chat-completions stream and `message_stop` a
   Messages stream. A whole answer arriving more than 30 s after the
   intended time is late and stays completed.
6. **Record.** The request is one line of the cell's `attempts.jsonl`,
   with its step, `in_time` or `late`, status, finish reason and token
   counts, and no answer text.
7. **Cell report.** The request is one count on its step's
   `completed_in_time` or `completed_late` line, or on an error or
   censored line, of the cell's `report.json`, over the step's scheduled
   requests with a 95 percent binomial (Wilson score) interval; if it
   completed, it also raises the step's completion curve at its
   completion time and counts toward the step's throughput, the
   completions per second over the step's 480 s window.
8. **Pages.** The route's page pools that line over the cells that
   attempted the step. The index row for the route and step prints one
   column per outcome line, each the line's pooled count over the pooled
   scheduled count, in a table ordered alphabetically by provider, route
   and step, carrying counts with no outcome rate, interval or ranking.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#hosted-runs-and-the-evidence-files)
describes the probe, the ledger, each evidence file and the mock's shaped
form, and [its release section](docs/ARCHITECTURE.md#the-release-runner)
the profile's pins, the schedule, the ceiling rule and check, a cell's
outcomes and files, the per-step tables and the index.

## Sweeping an endpoint outside the instrument

`cmd/naivesweep` sweeps one OpenAI-compatible endpoint and reports what came
back, including responses that returned 200 and carried nothing. It is
reconnaissance, and nothing it prints is a published measurement. Its
configuration, its limits and what it redacts are in its own documentation:

```
go doc ./cmd/naivesweep
```

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
