# Running a hosted release

This document is for an engineer who measures a provider's API with the instrument. A route is one provider, one model identifier, one endpoint, one account tier and one request shape. A release is the routes of one committed schedule, published together on one dated page.

A route's measurement publishes as a characterization, with pooled figures, only with twelve counted cells (a cell is one run of the route's five load steps). A route with fewer publishes its cells labelled incomplete, and a release whose routes are all excluded still publishes its pages ([SPEC-HOSTED §9](../SPEC-HOSTED.md#9-required-artifacts-and-the-publication-rule)).

Every hosted run writes the evidence its figures are recomputed from. A release runs from first command to published page through the commands in the table under [A release](#a-release), which gives when each runs.

The protocol those commands carry out is [SPEC-HOSTED.md](../SPEC-HOSTED.md), and [ARCHITECTURE.md](ARCHITECTURE.md#the-release-runner) describes the code behind each command.

## Building the release runner

Two make targets build `percentes-release`, the command that runs a release. The client host is the one machine a release's routes run on.

```
make percentes-release        # bin/percentes-release, the release runner for this host
make percentes-release-linux  # a static linux/amd64 build of it for the client host
```

## Operator rules

Keep `cmd/naivesweep` off release accounts during a release's collection window (the dates committed with its schedule). It is a reconnaissance probe outside the instrument and admits nothing to a release's ledger, so a sweep on a paid account spends outside every release cap.

Create the release key with `percentes-release key create` before the release's first ceiling check (ten requests that choose a route's output token limit), which sends the release's first paid request. A route with a confidential limit (one whose value only the account console shows, under terms that keep it confidential) needs the key for that check. The freeze is the commit, made before the collection window opens, that fixes the schedule; it comes after the ceiling checks and includes their records.

[A release](#a-release) gives the rules for keeping the release key file, for a confidential limit's value and for the admission ledger.

## What every hosted run writes

A hosted target (`target.hosted: true`) is a provider's managed
endpoint. SPEC-HOSTED.md pins what a hosted run sends and records. Two
configuration profiles carry those pins. `release` carries the adopted
protocol (hosted amendments 1, 3 and 4, HA1, HA3 and HA4, written into
its body) and is the path to published hosted figures. `pilot` carries
the numbers of a drafted amendment, HA2, that is not adopted, and
reports privately.

Each profile refuses a value outside its pins. Hosted runs go through
`percentes-pilot` or `percentes-release`, which reserve every request in
the admission ledger before it is sent; `percentes` and
`percentes-campaign` refuse a hosted target. A run's report pair is
`report.json`, the machine-readable report, and `report.txt`, its
human-readable rendering, from one configuration. Every hosted run
writes the evidence SPEC-HOSTED §9 requires:

- `attempts.jsonl`, one record per scheduled request;
- the raw response frames, in a store outside the source tree;
- the client clock offset, and the endpoint status at start and end;
- the pre-run probe's record;
- a manifest that hashes them all with the configuration, the operator's
  documentation records and the admission ledger's position.

A release cell adds four more records: the refresh record (the
cell-start run of the command that re-reads the account's limits and
model documentation pages), the balance records of its start and end,
the ceiling-check records of every route of the same host group (the
routes of a release that serve one model with one request shape) and,
for Shape S, each other route's record given to `run` that spent a byte
cap, and its dispatch series (dispatches per second of Coordinated
Universal Time, UTC, a write the write limit refused counted as a
dispatch with no byte sent).

Once no cell is left, the route's process writes the route's
window-close record, `window-close.json`: the limits and model
documentation read once more and compared with the last counted cell; a
stopped route's process writes it when it next runs. The report pair is
rendered from those files alone. Each cell's published files are one
compressed archive, `<route>-<cell>.tar.gz` (for example
`azure-weekend-08-a2.tar.gz`, the second attempt at a weekend 08:00 UTC
cell), on a GitHub release of this repository; given the release's
download address with `--cell-files`,
`percentes-release pages` writes each archive's address into its cell's
section of the route page.

## A release

`percentes-release` runs a public descriptive release under SPEC-HOSTED
amendments HA3 and HA4. A route is one provider, one model, one
endpoint, one account tier and one request shape; each route has one
configuration file, which carries the complete request uniform resource
locator (URL), the whole request body as literal JavaScript Object
Notation (JSON) frozen by hash, the header the key travels in, and every
limit recorded for the account with its source and a marking: `public`
where a public documentation page gives the value, which then publishes
in full, or `confidential` where only the account console shows it under
terms that keep it confidential.

A confidential limit's value is absent from the configuration as
published, the verdict of the ceiling rule (the tests of the route's
rates against its recorded limits, in the Binding rate row below), the
refresh records and the ceiling-check snapshots. The configuration and
the verdict name such a limit by its source and the keyed hash of its
retained copy.

A keyed hash here is an HMAC-SHA256 (a hash-based message authentication
code built on SHA-256, the 256-bit Secure Hash Algorithm) under the
release key: 32 random bytes that `percentes-release key create` writes
once, at the freeze, to a file only its owner can read. Nobody without
the key can check a guess against a keyed hash. Keep the key file
private, as the API keys are: copy it to the client host, and leave it
out of the repository.

Never write a confidential limit's value, in any format, into a
configuration comment, the operator file, a note, a URL or a file name.
A provider can echo a limit in its rate-limit headers or in the body of
its limits page, so a route with a confidential limit must withhold
both: `target.retained_headers` must cover every recorded response
header other than `Retry-After` and a request identifier such as
`X-Request-Id`, and `target.probe.limits_retained` must be set where the
route reads a limits URL.

On such a route the probe and ceiling-check records also carry each
non-200 response body as its keyed hash, and those records and the
attempts file carry each in-band error object as a stand-in with its
keyed hash. The texts stay in the retained store.

Each cell's manifest names the frame store, the operator file and its
records by keyed hash, with no byte count, and the route's window-close
record names the operator file and its records the same way; the
manifest names its other files by SHA-256 hash and size.

The routes of a release run as separate processes on one client host,
each at its own offset inside the slot: 0, 40, 80 minutes and so on, one
per route, with the routes of one account group (the routes recorded
against one provider account or deployment) at least 80 minutes apart
both ways round the 240-minute slot.

| Setting | Value |
|---|---|
| Ladder | five steps at 0.25, 0.5, 0.75, 0.9 and 1.0 of the route's binding rate, paced arrivals (requests at equal intervals), 240 s each, with a 90 s drain after every step in which nothing is sent, after a 60 s warm-up; a segment of T seconds at rate r sends floor(r·T) requests, r × T rounded down |
| Cell | one run of the ladder: 28 minutes 30 seconds; a cell that starts more than 15 minutes after its slot plus the route's offset, or whose ladder would start more than 15 minutes after that time, is missed |
| Frame | twelve cells per route: six UTC start slots (00:00, 04:00, 08:00, 12:00, 16:00 and 20:00), each run once on a weekday and once on a weekend day, inside a committed window of at most 14 days; a cell's first attempt is never on the last date of its day type in the window |
| Deadline and hard timeout | 30 s from intended dispatch splits completions into in time and late; 90 s from actual dispatch censors a request with no terminal event |
| Writes | a request more than 50 ms past its intended send time is not sent and counts as unsent, failing G2 at its step; in each evaluation window of W seconds of the route's limits, at most the lesser of the top step's rate times (W + 0.1) seconds, rounded up, and the requests the limits admit in W, at their recorded values and as read at the cell's start, are written, and for a limit marked `output_at_generation`, in each window of W + 90 seconds, at most the lesser of the top step's rate times (W + 90.1) seconds, rounded up, and that limit's allowance for W; the test is made as each request's first byte goes to the socket, a request counts in a window until the window's length plus 50 ms after its last byte, and a request the windows do not admit waits up to 50 ms from that first attempt; a request still not admitted is refused: it is not written, its record carries `write_denied`, the ledger does not settle it as sent, and it fails G2 at its step. A warm-up request left unsent or refused fails G2 at the first step. A request's write time is when the socket accepted its last byte |
| Request shape | Shape S: one frozen body per route with a synthetic support ticket per request, under a 1,200-token input bound and a 512- or 1,024-token output ceiling; Shape R, a probe of request-count limits: a frozen one-line instruction per request, under a 128-token input bound and a 16-token output ceiling, with a 256-token fallback where the route declares one |
| Binding rate | the highest rate, rounded down to 0.001 requests per second, at which nominal demand is at most every recorded rate, token, concurrency and spend-window limit in each evaluation window the provider documents, a limit marked `output_at_generation` (one the provider may count a request against until the request ends) over the window plus 0.1 s plus the 90 s hard timeout, on the assumption that the provider stops counting a request when the client gives up on it, and every daily limit over each of the provider's reset days (the period between two of its daily-quota resets), as a reservation of every attempt that can touch the day; the account's monthly cap and the route's liability cap must cover twelve cells, two re-runs and 100 probe requests, the auto-reload threshold the costliest cell, and the routes of one account group carry liability caps whose sum is at most the group's recorded spend limit; the limit or cap that sets it is the route's binding limit; a confidential limit never sets it, and a route whose confidential limit fails at the binding rate is excluded |
| Before each cell | the route's refresh command rewrites the limits and model documentation records with the lines it reads from the provider's pages for the route's tier and model (SPEC-HOSTED §2) and prints the limit values it read, each page's whole text staying beside the records, and its balance command reads the account's balance; a cell is blocked before any request where the clock-sync command gives no offset reading, its records were not rewritten, its balance is below the cell's reservation, the balance command fails, or the balance is unavailable and the route records no `balance_unavailable` ruling, and a route whose limits as read, at its newest recorded price, no longer admit one of its step rates, under the ceiling rule, stops; a limit the command could not read keeps its recorded value |
| After the last cell | the refresh command runs once more; its record is the route's window-close record, compared with the last counted cell and printed on the route's page |
| Model identifier | each response's reported model is compared with the expected one (the requested identifier, or the one `target.reported_model` records from the model documentation); a cell with any other is labelled `model_mismatch`, kept out of the frame and re-run, and the route's page lists it with the expected identifier, every identifier reported and how many requests reported another |
| Retries | none; `Retry-After` is never honoured, and a release configuration must record it under `target.response_headers` |
| Retention | the frame store keeps the model's answers, unpublished, for 12 months, and only where the route's recorded terms permit it (`target.retention`); otherwise it counts their bytes and keeps none |

Every finished cell writes a labelled manifest and report pair, except
a stopped cell whose evidence is not all written, which keeps its run
record; a cell that does not count is kept as a record outside the route's frame.

| Command | When | What it does |
|---|---|---|
| `percentes-release key create`, `percentes-release key sum` | before the window, before `ledger create` | `key create` writes the release key to a new file readable by its owner only, and refuses an existing file; `key sum` prints each named file's keyed hash, the `hmac_sha256` a confidential limit records for its retained copy; `schedule` and `run` read the key from `--release-key`, as `ceiling-check`, `pages`, `pack`, `publish-check` and `percentes --check-release` do where a route has a confidential limit |
| `percentes-release ledger create` | before the window | starts the admission ledger the routes share, with every route's liability cap |
| `percentes-release ledger price` | before the window | records a route's list prices at the token bounds of its cells and of its ceiling check; the ledger admits a request only at a bound it holds a price for |
| `percentes-release ceiling-check` | before the window, the checks of one account group one at a time | sends a route's ten-request ceiling check, admitted in the ledger with its ceiling and byte cap, and writes the record `run`, `schedule` and `pages` read before it settles the admission, settling as sent only the requests that reached a connection; it needs `--frames` where the route withholds probe values, `--release-key` too on a route with a confidential limit, and `--snapshots` where a limit's source is a file; it prints `REFUSED` for a refused check (SPEC-HOSTED §2) with the time from which it may be repeated once at that ceiling, or, where it repeats a refused check, that it excludes the host group, `BILLING`, `BLOCKED` or `STOPPED` for a check that decides nothing and may be sent again once the cause is fixed, and `NOT CHARACTERIZED` for a record reporting a model identifier on which `run` refuses the route; `ceiling-check --recover` sends nothing and settles each unsettled check of the route whose process ended, writing an interrupted record for any that has none, and `--recover --record FILE` writes a record the command printed on standard error when its write failed; exits 2 when the check fails, is refused, billed, blocked, interrupted or stopped, or reports such a model identifier; [docs/ARCHITECTURE.md](ARCHITECTURE.md#before-the-first-cell) lists each refusal and each exit 1 |
| `percentes-release schedule` | before the window, after the ceiling checks | writes the committed schedule of every route from one master seed and the window's dates, given with `--ceiling-probe` the record of every ceiling check the ledger admits; it reads each route's ceiling choice from those records at the release's byte cap, the least cap of a Shape S record that was not stopped, as `run` reads them, and refuses a route with no record of its host group or whose records choose a ceiling other than its configuration's; an excluded route stays in it and runs no cell, `--excluded` must name exactly the routes the records exclude, or `schedule` writes nothing and names each route that differs, and it prints for each excluded route a line starting `<route>: excluded by the ceiling check: <reason>`; it freezes each route's configuration as published and the ledger by their SHA-256 hashes, the configuration as loaded by its keyed hash, each route's host group, binding rate and binding limit, and the ledger's line count, so every start of a route takes its frozen binding rate and step rates at the prices recorded before the schedule; it refuses routes of one account group whose liability caps sum above the group's recorded spend limit, and draws the offsets again, with a counter it commits, until the routes of each account group start at least 80 minutes apart both ways round the slot; `run` refuses any other at the route's start, and checks the ledger's first line again at each cell; it never replaces an existing file |
| `percentes-release public-config` | at the freeze | writes a route's configuration as published, each confidential limit's value replaced by the quoted string `"[retained]"`; the freeze commits that file, and every other command reads the file as loaded |
| `percentes-release run` | from the first scheduled slot, one process per route | runs the route's cells over the window and writes each cell's evidence, labelled manifest and report pair, each cell's run record and manifest carrying the SHA-256 hash of the schedule file it ran under; at each cell's start it reads the clock and runs the refresh and balance commands, blocks the cell where the clock gives no reading or either command does not let it start, and stops the route where the limits read no longer admit a step rate, or after a cell in which a request reported more prompt tokens than its shape's input bound or more output tokens, reasoning included, than its ceiling; a stop exits 3, as does every later start until that stop, or an operator abort's marker, has its acknowledgement file under `--ack-dir`, a directory outside `--out` that the route's user can read and search and cannot write; a cell that SIGTERM interrupted before its outcome was decided is labelled at the next start and re-run on its attempt 2 date within SPEC-HOSTED §4's bound; once no cell is left it writes the window-close record and exits 0 where every cell counts and 4 where one does not; it exits 5 where SIGTERM ended it with no stop found; [docs/ARCHITECTURE.md](ARCHITECTURE.md#before-the-first-cell) lists each refusal to start |
| `percentes-release dispatch` | after the window | sums every cell's dispatch series into the host's dispatches per second |
| `percentes-release pages` | after the window | writes the index page and each route's page from the cells' records and each route's window-close record; labels each `status_429` on a route's page from its account group's send history, reconciled with the release ledger given with `--ledger`, which must be the one the schedule froze, and with every ceiling-check record no cell carries, given with `--check-record`; prints documented billing refusals on a `billing_refusal` line and states a complete frame's null reading at the 0.9 and 1.0 steps; a route of the schedule that ran no cell, and one with no cell manifest and a stopped attempt whose evidence was not all written, each get a page stating why and a line after the index table (SPEC-HOSTED §8), and `pages` writes the ceiling-check records such a page names under `--out/<route>/`; given `--cell-files`, the https address the cell archives are downloaded from, with no trailing slash, each route page names the archive of every cell with a manifest, `<address>/<route>-<cell>.tar.gz`, in that cell's section; [docs/ARCHITECTURE.md](ARCHITECTURE.md#the-per-step-tables-and-the-index) lists each refusal |
| `percentes-release pack` | after every route's window-close record and the last `pages` run, before the notices | writes a route's evidence pack, the tar archive file its notice carries: `inventory.json`, which names by SHA-256 the route's page files, its cell manifests and, for a route that ran no cell or has a stopped cell and no cell manifest, the ceiling-check records `pages` writes under `--out/<route>/`, and those files, built from the cells as `pages` builds them, with the same `--ledger` and `--check-record` files and `--cell-files` address; the same cells give the same bytes; it refuses a route with a page of its cells and no window-close record, and needs `--release-key` where a route has a confidential limit |
| `percentes-release publish-check` | before publication | passes only once the release publication record, which holds each provider's notice and response, is complete and agrees with the cells, the evidence packs, the window-close records and the pages, and the publication page it builds holds no recipient, account identifier or confidential value; it takes `--ledger`, `--check-record` and `--cell-files` as `pages` does, and `--write-page` writes the publication page under `--pages`; exits 2 while the record is incomplete or does not agree; SPEC-HOSTED §9 lists what it checks, and [docs/ARCHITECTURE.md](ARCHITECTURE.md#the-per-step-tables-and-the-index) lists each refusal |
| `percentes-release rehearse` | before the window, on the client host | the acceptance run: each of five mock routes, which the command serves itself on loopback over Transport Layer Security (TLS), the two Azure routes on one mock in one account group, gets its ten-request ceiling check, the Azure routes' checks one after the other, `azure-R` a second at its 256-token fallback, and then one cell, run at their offsets up the whole ladder; then an attribution cell under a mock limiter set below the `azure-R` route's binding limit, a cell of one 240 s ladder step at 1.5 times the `anthropic` route's top-step rate, one `anthropic` cell up the whole ladder with the mock's 429 and silent-hang faults armed in its top step, and a check-only step whose ceiling checks take the fallback and exclusion paths; `--anthropic-rpm` sets the `anthropic` route's requests-per-minute limit to 10,000, the default, or 5,000; writes each route's configuration as frozen to `inputs/<route>.yaml`, the file a replay of its cells reads, and `rehearsal.json`, which records the local port range and the `tcp_tw_reuse` setting and says whether the rehearsal is green (among [its checks](ARCHITECTURE.md#the-rehearsal), every route's cell of the schedule counted with G2, the check that the client machine was not the bottleneck, passing at every ladder step; the attribution cell recorded at least one `status_429` and its page labelled every injected 429 at or below the limit's share, and one of them comparison unresolved once its request's write time was removed from the history; the attribution, 1.5 times and failure-mode cells counted and attempted every ladder step with G2 passing at each and no ladder end; the check-only step recorded the fallback choice and the exclusion's reason; every cell rendered its report pair again byte for byte from its route's `inputs/<route>.yaml`; the client host stayed within its resource bounds; and a window's disk, projected from each route's largest rehearsal cell at least two times over, fits the free disk of `--out` and `--frames`, taken together where they share a filesystem); at full length, the default, that file is the run's evidence |
| `percentes --replay`, `percentes --check-release` | any time after a cell | render a cell's report pair again and compare it byte for byte; refuse a cell whose files, the unpublished frame store among them, are missing or differ from their recorded hashes |

Use one live ledger for all release traffic; do not run an independent
copy or restore an older copy of the ledger file. Reconcile any recovery
against the complete retained admission history before resuming.

```
go doc ./cmd/percentes-release
```
