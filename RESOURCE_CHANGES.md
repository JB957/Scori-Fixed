# Scori-Fixed resource changes

This document compares Scori-Fixed with the adjacent, untouched `Scorify` checkout at commit `8dd52e2`. The Scori-Fixed branch already contained commit `88f416f` (`Resource effeciancy`) before this work. The application now builds all registered checks from `pkg/localchecks`. The eight original sibling check clones were moved out of the workspace to `/private/tmp/Scorify_Fixing_check_source_backups_20261018` so their Git histories remain recoverable while the source now lives in Scori-Fixed.

## Changes already present in Scori-Fixed

| Area | Existing branch change | Review result |
| --- | --- | --- |
| Worker lifecycle (`pkg/cmd/main.go`, `pkg/cmd/minion`, `pkg/cmd/koth`) | SIGINT/SIGTERM cancel command contexts. KOTH stops and waits for its scoring goroutine, and the minion limits concurrent checks to 4–32. | Kept. Minion still used fatal exits on recoverable listener failures and could overlap scoring loops; both were repaired. KOTH now restarts if its listener exits unexpectedly. |
| Server and database (`pkg/cmd/server`, `pkg/data`) | Server uses a graceful HTTP shutdown, closes Redis/Postgres clients, buffers work channels, and caps the Postgres connection pool at 20 open / 5 idle. | Kept. HTTP server exit now cancels the rest of the server so a failed bind cannot leave the process waiting forever. |
| Scoring (`pkg/engine`, `pkg/helpers/scores.go`) | Round task submission uses two producer goroutines, status writes have bounded concurrency, and score recomputation writes one round at a time. | Kept. A status was removed from the pending map before its database write succeeded; this could leave an unknown status carrying points. It is now removed only after a successful write. Empty bulk creates and per-status timeout updates were also fixed. |
| RabbitMQ (`pkg/rabbitmq/rabbitmq`) | Partially opened connections and channels are closed, consumers detect closed delivery channels, task/response consumers use manual acknowledgements and QoS, and all server workers are joined on cancellation. | Kept. AMQP connection setup now uses escaped credentials and a 10-second connection/handshake limit to avoid long startup stalls. |
| RabbitMQ management (`pkg/rabbitmq/management`) | HTTP requests close response bodies and use a bounded connection pool and a 10-second client timeout. | Kept. |
| GraphQL subscriptions (`pkg/graph/schema.resolvers.go`) | Redis channel closure is handled instead of looping on empty messages. | Kept. |
| Frontend (`src/pages/Scoreboard/*`, `src/pages/Statuses.tsx`) | Removed duplicate and render-triggered query refetches. | Kept. |

## Changes made in this pass

| Area | Change and reason |
| --- | --- |
| Check ownership (`pkg/localchecks`, `pkg/checks`, `config.yml`, `tools/checkgen`, `main.go`, `tools.go`, `go.mod`) | All 14 registered checks are now local packages. Eight came from the sibling check repositories; the other six came from their pinned Go module versions. Registry generation uses the local configuration and never downloads check modules. The old remote check requirements and generator dependency were removed. Run `go generate .` after changing `config.yml`. |
| FTP, SSH, WinRM, Ping checks | Removed detached timeout wrapper goroutines. FTP and SSH operations use network deadlines and cancellation to close connections; WinRM uses its context-aware call directly; Ping stops and joins its run before returning. |
| HTTP, FTP, SMB, SSH, NC, WinRM, KOTH checks | Limited response or file data retained in memory to 1 MiB. Existence checks do not read file content. The KOTH response now includes its minion ID. Oversized content reports a failed check instead of growing memory without bound. |
| IMAP, LDAP, SMTP, SMB, NC checks | Added deadline or cancellation-driven connection closure around protocol operations, including login and command phases. This prevents stalled peers from retaining workers and sockets indefinitely. |
| HTTP and MySQL checks | HTTP reuses bounded idle transports instead of allocating one transport per check. MySQL uses a correctly escaped driver DSN with subsecond timeouts, one connection, no idle connection, and checks row errors. |
| Minion and KOTH workers | Listener failures now trigger controlled retries. Disabled workers wait for scoring to stop before restarting it. KOTH files have a 1 MiB cap. |
| Engine | Failed status writes remain pending for final unknown/zero-point handling. Missing statuses are zeroed with one bulk database update instead of one round trip per status. Error text is capped at 4 KiB. Empty task or score sets no longer call empty bulk inserts. |
| Resource telemetry (`pkg/rabbitmq/rabbitmq/heartbeat.go`, `pkg/cache/minion.go`, `pkg/structs/rabbitmq.go`, `src/components/Admin/Minions/EditMinion.tsx`) | Heartbeats report the minion process RSS and its CPU use instead of host active memory/CPU. The UI labels RSS against host RAM. Heartbeat keys expire after 30 seconds, group lookup uses Redis SCAN instead of KEYS, and byte formatting is corrected. |
| Frontend runtime (`src/App.tsx`, `src/Router.tsx`) | Apollo and WebSocket clients remain stable across theme renders, avoiding duplicate subscriptions. The browser router is created once and receives live theme/engine state through context, so round updates do not remount pages. Route pages load on demand to reduce initial transfer size. |
| Browser engine state (`pkg/cmd/server/main.go`) | Allow the configured domain on the default HTTP and HTTPS ports for WebSocket upgrades. Caddy serves the frontend at `https://localhost` locally, while the earlier origin list allowed only the backend's `:8080` port; the browser subscription was rejected with 403 and displayed `disconnected`. |
| Scoreboard countdown (`src/pages/Scoreboard`, `src/components/Scoreboard/RoundCountdown.tsx`, `pkg/engine/main.go`, `pkg/cmd/server/main.go`) | Current and historical normal and KoTH scoreboards show seconds until the next round. The server reports remaining time from the engine's actual ticker, so the display stays aligned after refreshes and shows when the engine is paused. Stopping the engine cancels its ticker loop immediately, and a stale loop cannot run a round or overwrite state after a quick restart. The frontend build regenerates GraphQL types from the backend schema before compiling, with npm dependencies cached across source edits. |
| File storage (`pkg/structs/file.go`) | Upload writes always close their file handle and report close errors. |
| Minion identity (`pkg/config/main.go`, `docker-compose.yml`) | The minion ID file is read and written without leaked file handles, and Compose persists it in a named volume. This prevents a new Postgres minion row each time the container is recreated. A missing local `.env` is no longer logged as an error when environment variables are already injected by Compose. |
| Profiling (`pkg/cmd/main.go`, `docker-compose.yml`, `docker-compose.profile.yml`) | Optional Go pprof endpoints expose heap, allocation, goroutine, mutex, block, CPU, and trace profiles. They are disabled by default. The Compose override binds profile ports to the host loopback interface. |
| Images (`Dockerfile.scorify`, `Dockerfile.frontend`) | The server/minion runtime uses Debian slim instead of a full Go SDK image, builds a stripped static binary, and the frontend uses reproducible `npm ci`. |
| Deployment docs (`README.md`) | Quick start points at Scori-Fixed and rebuilds images before starting services. |

## Checking memory and latency

The admin Minion page shows current process RSS, CPU, and heartbeat time. A rising RSS trend across repeated idle periods is a useful signal, but one sample does not prove a leak. `docker stats scorify-minion scorify-server` also shows container memory, CPU, and process counts.

For Go profiles, start the optional local-only ports:

```sh
SCORIFY_PPROF_ADDR=:6060 docker compose -f docker-compose.yml -f docker-compose.profile.yml up -d --build
go tool pprof -http=127.0.0.1:8081 http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -http=127.0.0.1:8082 http://127.0.0.1:6061/debug/pprof/heap
```

Port 6060 is the minion; 6061 is the server. Restrict access to these endpoints because profiles can contain sensitive data. Compare heap and goroutine profiles before and after several rounds, preferably after forcing a GC with `/debug/pprof/heap?gc=1`. To disable profiling, restart with the normal `docker-compose.yml` and no `SCORIFY_PPROF_ADDR`.

## Behavior changes to account for

- A check that needs to compare more than 1 MiB of HTTP, FTP, SMB, SSH, NC, WinRM, or KOTH content now fails with a size error. Split or narrow that check if larger content is necessary.
- A minion with no heartbeat for 30 seconds has no cached metrics. The minion record remains in Postgres.
- When upgrading an existing Compose minion to the persistent ID volume, copy its current `.minion` value into `MINION_ID` in `.env` before the first recreate. The minion writes that value to the volume and keeps it across later rebuilds. New deployments generate the ID automatically.
- The optional profile override uses localhost ports 6060 and 6061.

## Verification

Verified locally on 2026-10-18: `go generate .`, `go test -race ./...`, `docker compose config --quiet`, `git diff --check`, and Docker builds for frontend, server, and minion all passed. The final images were deployed with Compose; all six services were Up. HTTPS frontend and HTTP server returned 200, and `POST /api/query` returned GraphQL data. The minion sent repeated heartbeats and kept the same ID after a Compose restart. The adjacent `Scorify` checkout stayed clean.

The scoreboard countdown was additionally tested with `INTERVAL=30s`: the timing API reported 30, 29, and 28 seconds; it reset to 30 after a quick stop and restart; and exactly one new round was created at the next tick before the countdown reset again.

Protocol checks were compiled and covered by the race suite, but live FTP, SMB, SSH, WinRM, and other remote endpoints were not available for end-to-end probes. OpenStack-specific latency and long-running leak behavior still need measurement on an OpenStack deployment; local Compose verifies startup and service wiring only.
