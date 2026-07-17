# Opera Stream Consumer

A Mule application that consumes Oracle OHIP (Opera Cloud) Business Events from the OHIP Streaming API, following Oracle's documented best practices. It is a reference-quality starting point for MuleSoft developers.

## Language

**Stream**:
The single long-lived `graphql-transport-ws` WebSocket connection over which OHIP delivers Business Events. Identified by the tuple (**Application Key**, gateway URL, **Chain**).
_Avoid_: socket, channel, feed.

**Business Event**:
A notification that a resource in Opera Cloud was created, updated, or deleted. Carries an **Offset**, a **uniqueEventId**, the module/event name, and a `primaryKey` identifying the changed resource. May also carry a `detail` array of changed elements, but under **Orchestration** the template omits `detail` at subscribe time and re-fetches current state via REST instead.
_Avoid_: message, notification, record.

**Chain**:
The Opera Cloud tenant scope a subscription targets, addressed by its `chainCode`. Streaming is enabled at the Chain level and applies to all its Hotels.
_Avoid_: tenant, org, property group.

**Hotel**:
An individual property within a **Chain**. Addressed by `hotelCode` in the **subscribe** filter (`NewEventInput`), but reported as `hotelId` in the **Business Event** header. `hotelId` is null on chain-level events (e.g. profiles).
_Avoid_: property, site, location.

**Application Key**:
The credential identifying the consuming application. Its SHA-256 hash is passed as the `?key=` handshake query param. Scopes the **Single-Consumer Lock** together with the **Chain**.
_Avoid_: client id, api key (the OAuth `clientId`/`clientSecret` are separate).

**Offset**:
A string (schema pattern `^[0-9]+$`, max 20 chars) marking a position in the **Stream**, used to replay **Business Events**. **Opaque and NON-MONOTONIC**: Oracle states "while these appear to increment, a linear progression is not guaranteed" (`Oracle's Streaming API Guide`), and the value can change after a disconnect longer than 24h. Stored and replayed verbatim as a string — never parsed to an integer (it overflows), never assumed to progress by +1, and never compared (no `if event.offset > lastApplied` ordering logic). Retained by OHIP for 7 days.
_Avoid_: sequence number, cursor, position (use the exact term).

**Orchestration**:
The design (Oracle's "Details Steps for Orchestration") where a **Business Event** is treated as a TRIGGER rather than a source of truth: the consumer uses `eventName` + `primaryKey` to re-fetch the current resource state via OHIP REST (e.g. `getProfile`, `getReservation`), then upserts that latest state. Because the latest state is always read, message order and duplicate delivery stop being correctness concerns. Trades away field-level change history (the `detail` array) for throughput and reorder/dup immunity, at the cost of a REST dependency, rate limits, and read-after-write lag. See the design notes.
_Avoid_: enrichment, lookup, hydration (use the exact term).

**uniqueEventId**:
A UUID on each **Business Event** used for idempotency/deduplication, persisted alongside the **Offset**.
_Avoid_: event id, message id.

**Single-Consumer Lock**:
OHIP's rule that only one active consumer may hold a given (**Application Key**, **Chain**) **Stream**. A second consumer's event **subscribe** is rejected with close code `4409`. The lock is on the **subscribe**, not the socket — multiple instances may open sockets and run the **Connection Status Check** concurrently; only the event `subscribe` contends.
_Avoid_: mutex, connection limit.

**Connection Status Check**:
The GraphQL query `query { connection { id status } }` a consumer runs (after `connection_init`, before subscribing) to learn whether the **Stream** is already held. Returns `status: "Active"` (a peer holds it — stand down) or `"Inactive"` (free — safe to attempt **Takeover**). The basis of Oracle's competing-consumer HA pattern (`Oracle's Streaming API Guide` "Implement a Connection Status Check"). Only meaningful in **HA Mode**.
_Avoid_: health check, ping, heartbeat (those are the keepalive `ping`/`pong`).

**HA Mode**:
The symmetric competing-consumer mode of the HA variant, toggled by `ohip.ha.enabled`. Off → the app subscribes immediately on `connection_ack` (base single-consumer behavior). On → the app runs a **Connection Status Check** and only subscribes when `Inactive`, coordinating with all other instances through OHIP (the status query + the `4409` lock), never through a shared store. All instances are **identical** — deploy the one artifact as N replicas of one CloudHub 2.0 deployment (auto-spread across availability zones) and/or in additional regions; OHIP's **Single-Consumer Lock**, being global to (**Application Key**, **Chain**), arbitrates across all of them. Superseded the earlier "deploy twice as active/standby" topology (the design notes over the design notes).
_Avoid_: cluster, failover mode, HA cluster, active/standby.

**Takeover**:
A passive instance's attempt to become the active consumer: on seeing `status: "Inactive"`, it waits a randomized delay (`ohip.ha.takeoverDelayMs` + jitter, identical across all instances, per `Oracle's Streaming API Guide` "Recommended Competing Consumer Pattern") and then sends the event **subscribe**. If it wins, **Business Events** flow; if it loses the race, OHIP closes `4409` and the standard 2min+jitter backoff applies before it re-probes.
_Avoid_: promotion, election, acquire.

**Backpressure**:
The mode OHIP enters when event volume is high, delivering **Business Events** in bursts and pacing itself by measuring ping/pong latency. Not client-configurable.
_Avoid_: throttling, rate limiting, flow control (use the exact term).

**Replay**:
Re-requesting **Business Events** from a stored **Offset** when reconnecting after a gap. Distinct from normal live consumption.
_Avoid_: catch-up, backfill, resync.

## Relationships

- A **Stream** is scoped by exactly one (**Application Key**, **Chain**) pair; only one active consumer may hold it (**Single-Consumer Lock**).
- A **Chain** contains many **Hotels**; a **Business Event** belongs to a **Chain** and optionally names a **Hotel**.
- A **Business Event** carries one **Offset** and one **uniqueEventId**; the producer persists both to enable **Replay**. Under **Orchestration** the consumer keys re-fetch and upsert on the event's `primaryKey`, so **uniqueEventId** dedup becomes an optional optimization rather than a correctness requirement.
- In **HA Mode** N identical instances (replicas within a region and/or deployments across regions) share one **Stream**: each runs the **Connection Status Check** and only attempts **Takeover** when it reads `Inactive`. Exactly one holds the **Single-Consumer Lock** at a time; the rest stay passive and poll. They coordinate only through OHIP, never a shared store.

## Example dialogue

> **Dev:** "When we reconnect after a five-minute drop, do we always send the stored **Offset**?"
> **Domain expert:** "No — within 24 hours OHIP still holds your position, so you reconnect without one. You only send the **Offset** for **Replay** after a gap longer than 24 hours, and it's only valid for 7 days."
> **Dev:** "And if another instance connects while one is already holding the stream?"
> **Domain expert:** "It hits the **Single-Consumer Lock** — close code `4409`. That instance must stay passive and poll connection status, never just retry blindly."

## Flagged ambiguities

- "app key" vs. OAuth credentials — resolved: **Application Key** (hashed into `?key=`) is distinct from the OAuth `clientId`/`clientSecret` used to fetch the bearer token.
- Oracle's guidance gives differing `4409` retry waits in different places (roughly 2–5 minutes) — resolved: template defaults to **2 min + jitter**, exposed as a configurable property.
- `hotelId` vs. `hotelCode` — resolved against the GraphQL schema: the **subscribe filter** field is `hotelCode`; the **event header** field is `hotelId`.
- **Offset** reads as non-numeric in some descriptions but the schema constrains it to the pattern `^[0-9]+$` — resolved: it is a numeric *string*, treated as opaque (stored/replayed verbatim, never parsed).
