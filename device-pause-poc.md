# Device-side Pause + varlink IPC — PoC summary

A proof-of-concept for the **PauseBefore** epic (MEN-9211): let a Mender device hold an
update at chosen points, and let an external agent observe and control that pause over a
**device-side varlink IPC** — without the device depending on any varlink library.

Branch: `worktree-pausebefore-daemon` (stacked on `master`). All commits local.

---

## 1. What it does

Two cooperating pieces:

1. **PauseBefore (daemon).** A `mender.conf` option makes the `mender-update` daemon pause a
   deployment *before* one or more states: `Download`, `ArtifactInstall`, `ArtifactReboot`,
   `ArtifactCommit`. While paused the deployment is held (no progress, no new-deployment
   polling), but **inventory keeps being submitted** and the pause **survives a daemon
   restart**. A configurable timeout auto-aborts a too-long pause.

2. **varlink IPC (device API).** The daemon exposes a unix-socket service,
   `io.mender.Update1`, so an external process can query state, **Continue / Abort /
   ExtendTimeout** a paused deployment, and **Monitor** a live event feed (including a
   push-based download-percentage feed — no polling).

There is intentionally **no built-in resume** other than the IPC — resuming/aborting a pause
is exactly what the device API is for.

---

## 2. Configuration

`/etc/mender/mender.conf`:

```json
{
    "PauseBefore": ["ArtifactInstall"],
    "PauseBeforeTimeoutSeconds": 604800,
    "ServerURL": "https://hosted.mender.io",
    "TenantToken": "…"
}
```

- `PauseBefore` — a state name **or** a list of state names. An invalid value or wrong type
  makes the daemon **fail to start** (loud, not silently ignored). Valid:
  `Download`, `ArtifactInstall`, `ArtifactReboot`, `ArtifactCommit`.
- `PauseBeforeTimeoutSeconds` — max seconds to hold a pause before auto-aborting. Default
  `604800` (7 days). Must be a positive integer.
- **Standalone** `mender-update install` ignores `PauseBefore` (daemon-only feature).

---

## 3. How varlink is used

### Why varlink, hand-rolled
The device speaks the **varlink wire protocol directly over a unix stream socket** — it does
**not** link any varlink library. The protocol is just newline-of-its-day **NUL-separated
JSON objects**, which is trivial to emit/parse with the JSON we already vendor. Consumers, on
the other hand, *may* use a real varlink library, or talk raw JSON (as the demo does).

This keeps the device dependency-light while staying interoperable with standard varlink
tooling — and it's deliberately **generic** (`src/common/varlink/` has no Mender-specific
types) so the same server could later host other interfaces and eventually **replace the
D-Bus API** (`io.mender.AuthenticationManager`). A `varlink::Client` (consumer side) is the
known next step for that, not built here.

### Wire format
- **Socket:** `/run/mender/update.sock` (mode `0660`), created at daemon start.
- **Call:** `{"method":"io.mender.Update1.GetState","parameters":{…},"more":false}` + `\0`
- **Reply:** `{"parameters":{…}}` + `\0`; streaming replies add `"continues":true`
- **Error:** `{"error":"io.mender.Update1.NotPaused","parameters":{}}` + `\0`
- **Signals** use varlink streaming: call a method with `"more":true` and the server keeps
  pushing `"continues":true` frames (that's how `Monitor` works).

### Interface `io.mender.Update1`

| Method | Params | Behaviour |
|---|---|---|
| `GetState` | — | Current snapshot: `status`, `state`, `paused`, `deployment_id`, `pause_deadline`, `download_progress`. |
| `Continue` | — | Resume a paused deployment. Error `NotPaused` if not paused. |
| `Abort` | — | Abort + roll back a paused deployment (paused-only). |
| `ExtendTimeout` | `seconds:int` | Push out the pause deadline. |
| `Monitor` | (call with `more:true`) | Stream a frame on every status change **and every download-% increase**. Without `more`, returns a single snapshot. |

Errors: `io.mender.Update1.NotPaused`, `…InvalidParameter`, and standard
`org.varlink.service.MethodNotFound`.

### Example session (what a consumer sees)

```jsonc
// GetState while downloading-then-paused
{"parameters":{"status":"in-progress","download_progress":42,"paused":false,…}}
// Monitor feed (more:true) — live download %, then the pause:
{"continues":true,"parameters":{"status":"in-progress","download_progress":99,…}}
{"continues":true,"parameters":{"status":"in-progress","download_progress":100,…}}
{"continues":true,"parameters":{"status":"paused","state":"ArtifactInstall","paused":true,"pause_deadline":1781629380}}
// Consumer decides:
{"method":"io.mender.Update1.Continue"}   ->  {"parameters":{}}   // resumes -> install -> reboot
```

### Two ways to talk to it (device links no varlink library either way)

**A. Raw socket / socat** — JSON objects over a `NUL`-framed unix socket:
```sh
printf '{"method":"io.mender.Update1.GetState"}\0' | socat - UNIX-CONNECT:/run/mender/update.sock
printf '{"method":"io.mender.Update1.Monitor","more":true}\0' | socat - UNIX-CONNECT:/run/mender/update.sock
```
(or the bundled `/root/mender_varlink_demo.py` — plain `AF_UNIX`, no library.)

**B. A real varlink client** — the socket speaks genuine varlink, so stock
`varlinkctl` (ships with systemd) drives it unchanged:
```sh
varlinkctl info        /run/mender/update.sock                    # vendor/product/interfaces
varlinkctl introspect  /run/mender/update.sock io.mender.Update1  # the IDL
varlinkctl call        /run/mender/update.sock io.mender.Update1.GetState
varlinkctl call -E     /run/mender/update.sock io.mender.Update1.Monitor   # -E = --more (live feed)
varlinkctl call        /run/mender/update.sock io.mender.Update1.ExtendTimeout '{"seconds":3600}'
```
`info`/`introspect` work because the daemon implements the standard
`org.varlink.service` interface (`GetInfo` + `GetInterfaceDescription`); the device
still links no varlink code. Same property that lets varlink eventually replace D-Bus.

---

## 4. How the daemon implements the pause

The deployment state machine (`src/mender-update/daemon/`) gets a `PauseState` inserted
before each of the four boundaries:

- On entry, if its boundary isn't in `PauseBefore`, it passes straight through.
- If it is: it **persists a marker** (`pause-before-install`, …) to the LMDB state data,
  sets `deployment.paused` / `paused_state`, arms the timeout timer, starts a
  while-paused inventory loop, and posts **no** progress event — the deployment holds.
- **Restart while paused:** `LoadStateFromDb` maps the marker back into the `PauseState`
  (instead of the power-loss → rollback path), so the deployment re-pauses.
- **Timeout:** an absolute deadline is persisted once, so a restart honors the *remaining*
  time. On expiry the deployment fails/rolls back through the normal per-stage path.

IPC control maps to state-machine events on the daemon's single event loop (same mechanism
the SIGUSR signal handlers use): **Continue → `Success`** (resume to the next work state),
**Abort → `Failure`**, **ExtendTimeout →** re-arm the timer + re-persist the deadline.

The **download-% feed** comes from `progress::Reader` (which already tracks each percent
increase) calling a callback → `Context::ReportDownloadProgress` → status observers → any
`Monitor` subscriber.

---

## 5. Code layout

```
src/client_shared/config_parser/        PauseBefore / PauseBeforeTimeoutSeconds parsing + validation
src/client_shared/conf/                 invalid value => fatal daemon startup
src/common/varlink/                      GENERIC varlink server (transport + framing + dispatch + streaming)
src/mender-update/daemon/
    context.*                            DeploymentStatusSnapshot, status observers, ReportDownloadProgress, pause flags, pause_deadline
    states.*                             PauseState (hold/passthrough, timeout, inventory loop)
    state_machine/state_machine.*        transitions, LoadStateFromDb restore, IPC control API (Resume/Abort/ExtendTimeout, QueryDeploymentState)
    ipc/update_service.*                 io.mender.Update1 service (binds methods/signals to the SM + Context)
    progress_reader/                     download progress callback
src/mender-update/cli/actions.cpp        starts the IPC service in the daemon lifecycle
```

---

## 6. Build, run, test

**Unit/integration (host):**
```bash
cmake -B build -D CMAKE_BUILD_TYPE=Debug
cmake --build build --parallel 8
ctest --test-dir build -R 'config_parser_test|conf_test|mender_update_state_test|varlink_server_test|update_service_test'
```

**On-device:** drive the socket with any of the three styles in §3 — e.g. `varlinkctl`,
raw `socat`, or the bundled demo script:
```bash
mender_varlink_demo.py            # read-only showcase (GetState + Monitor)
mender_varlink_demo.py monitor    # live feed (download % -> paused frame)
mender_varlink_demo.py continue   # resume   (also: abort, extendtimeout <s>)
```

---

## 7. Verified

- Compiles under the **release** flags (`-Werror -Wconversion -Wold-style-cast -Wsuggest-override`).
- Host suites green (daemon SM 117, varlink, update_service, config/conf).
- **End-to-end on real qemux86-64 hardware:** download-% feed streamed live, deployment
  paused before `ArtifactInstall` (`paused:true`, deadline set), `GetState` / `Continue`
  (resumed → installed → rebooted) / `Abort` / `ExtendTimeout` all behaved, socket `0660`.

## 8. Out of scope / next

- `varlink::Client` (consumer side) — required before retiring the D-Bus API.
- Server-side "paused" deployment status (PB-6); meta-mender `MENDER_PAUSE_BEFORE` recipe plumbing.
- Loop-detection hardening across *many* restarts during one long pause (PB-5).
- Cancelling an actively-running (non-paused) update; socket authz beyond filesystem perms.
