# Opera Stream Consumer — HA variant (symmetric competing consumer)

> **Setup.** The Quickstart, Configuration (baseline + tunables), Secure Properties setup, local
> simulator instructions, and developer plug-in point are shared with
> [`opera-stream-consumer`](../opera-stream-consumer/README.md) — follow that README for them. This
> README covers the HA behavior specifically.

This app is a **purpose-built HA** consumer: run N identical replicas against one (Application Key,
Chain), and OHIP's Single-Consumer Lock keeps exactly one of them subscribed at a time. It implements
Oracle's **connection-status-check + jittered-failover** pattern from Oracle's Streaming API Guide in
[`src/main/mule/ha-failover-flow.xml`](src/main/mule/ha-failover-flow.xml), whose
`ha-failover-controller-flow` is the **single driver** of every socket open: it probes connection
status, stands down while a peer is active, and takes over when the stream goes free. A dropped socket
clears the holder's state, and the controller re-probes on its next poll tick — the poll interval is
the sole recovery cadence.

For a plain single-instance producer, use the base [`opera-stream-consumer`](../opera-stream-consumer/README.md).

## Architecture

```mermaid
flowchart TB
    classDef muleFlow fill:#0C2340,stroke:#00A9E0,stroke-width:2px,color:#ffffff
    classDef oracleNode fill:#ffffff,stroke:#C74634,stroke-width:1.5px,color:#0C2340
    classDef muleNode fill:#ffffff,stroke:#00A9E0,stroke-width:1.5px,color:#0C2340

    subgraph OHIP["Oracle OHIP (Opera Cloud)"]
        Lock{{"🔒 Single-Consumer Lock<br/>global to (Application Key, Chain)"}}:::oracleNode
        Stream(["📡 Streaming API<br/>graphql-transport-ws"]):::oracleNode
        Lock -.arbitrates subscribe.-> Stream
    end
    style OHIP fill:#ffffff,stroke:#C74634,stroke-width:1.5px

    subgraph InstanceA["Instance A (identical artifact)"]
        ProbeA["ha-failover-controller-flow<br/>connection-status poll"]:::muleFlow
        BaseA["connect / keepalive (15s ping) / token-refresh<br/>+ on-socket-closed (classify only)"]:::muleFlow
    end
    style InstanceA fill:#ffffff,stroke:#00A9E0,stroke-width:1.5px

    subgraph InstanceB["Instance N (identical artifact)"]
        ProbeB["ha-failover-controller-flow<br/>connection-status poll"]:::muleFlow
        BaseB["connect / keepalive (15s ping) / token-refresh<br/>+ on-socket-closed (classify only)"]:::muleFlow
    end
    style InstanceB fill:#ffffff,stroke:#00A9E0,stroke-width:1.5px

    Queue[("Event queue: Anypoint MQ<br/>standard queue")]:::muleNode

    ProbeA -.query connection status.-> Stream
    ProbeA -->|status Inactive: jittered subscribe| Lock
    BaseA -->|holds subscription| Stream
    Stream -->|Business Events| BaseA
    BaseA -->|publish| Queue

    ProbeB -.query connection status.-> Stream
    ProbeB -->|status Inactive: jittered subscribe| Lock
    BaseB -.stands down while peer Active.-> ProbeB
```

## Why and when to use this

OHIP Streaming API enforces a **Single-Consumer Lock**: only one active **event subscription** per (Application Key,
Chain) is allowed; a second one is rejected with close code `4409`.

To avoid downtime from a stream consumer failure, such as an AWS availability zone or region outage, this app can be deployed across multiple replicas in one region or across regions.

## Deployment topologies

For the CloudHub 2.0 deployment model:
- To achieve in-region high availability, select 2 replicas at deployment. This automatically distributes the app across multiple availability zones in AWS.
- To achieve cross-region high availability, deploy this app in 2 different CloudHub 2.0 regions. When you create your Anypoint MQ queues in the admin console, ensure "Cross-Region Failover" is enabled.

**Note: scaling the stream consumer app to multiple replicas does not improve performance. Only one replica will ever be connected to the OHIP Streaming API at any given time.**

## How it works

Run N of them against the same
(Application Key, Chain), any way you like (see [Deployment topologies](#deployment-topologies)). Each
instance, while it does **not** hold the stream:

1. Opens the Stream socket and sends `connection_init` (`connect-subflow`).
2. Runs `query { connection { id status } }` (`ha-send-status-query-subflow`).
3. On `status == "Inactive"`, waits a **jittered delay**, then sends the event `subscribe` to take over.
   On `status == "Active"`, stands down and keeps polling.
4. Whoever's `subscribe` lands first wins the lock. A loser gets `4409`; OHIP closes its socket, which
   clears its `ha-subscribed` state, and the **same controller** re-probes on its next poll tick — now
   sees `Active` and stands down. The poll interval is the only retry cadence.

**Symmetric jitter:** every instance uses the same `ohip.ha.takeoverDelayMs` (default 1s) plus a random
`0..takeoverJitterMaxMs`, so a tie between any number of instances resolves randomly without a `4409`
storm. No instance is privileged. When the holder dies, the first surviving instance to observe
`Inactive` on its poll takes over within `takeoverDelayMs + jitter`.

## Configuration

All HA knobs are in [`config.properties`](src/main/resources/config.properties) under the HA block and
overridable at deploy time (`-Dohip.ha.*`):

| Property | Default | Purpose |
|---|---|---|
| `ohip.ha.statusPollIntervalMs` | `60000` | How often a passive instance re-checks connection status, and — since the controller is the only retry driver — the max latency before a dropped holder is picked back up. Failover ≈ this + `takeoverDelayMs` + jitter. Events queue up to 7 days in OHIP, so a slower poll costs failover **latency, not data**. Push higher if minutes-scale failover is acceptable. |
| `ohip.ha.takeoverDelayMs` | `1000` | Base delay before an instance subscribes after seeing `Inactive` (same for all instances). Raise it on a non-preferred region's deployment to bias a preferred region. |
| `ohip.ha.takeoverJitterMaxMs` | `4000` | Random jitter added to the takeover delay (breaks ties, per Oracle's Streaming API Guide). |

> **The state store is per-instance on purpose.** The HA coordination state (`haStateOs`) is per-instance
> and non-persistent by design — instances coordinate through OHIP (the status query + the `4409` lock),
> not through a shared store. That is what makes scaling to N replicas safe: each replica competes
> independently and OHIP is the single coordinator, so there is no shared state to keep in sync and no
> split-brain to manage.

## Demo failover against the local simulator

The simulator ([`../opera-stream-consumer/sim/ohip-sim.js`](../opera-stream-consumer/sim/ohip-sim.js))
enforces the lock at the **subscribe** level and answers the status query, matching real OHIP — so you
can watch a real handoff locally (the two instances are identical and fully interchangeable):

1. Start the sim: `node ../opera-stream-consumer/sim/ohip-sim.js`
2. Start **instance A** (with `-Dmule.key=…`).
   Watch: connect → `connection_ack` → status query → `Inactive` → subscribe → Business Events flow.
3. Start **instance B** (same artifact, same command). Watch: it connects and polls, sees
   `Active` (A holds the lock), and **stays passive** — no `4409` storm.
4. Kill instance A (or `curl "http://localhost:8081/control/close?code=1000"`). On B's next poll it sees
   `Inactive`, waits `takeoverDelayMs + jitter`, subscribes, and starts receiving events.
5. Restart A. It probes, sees `Active` (B now holds it), and stays passive until B drops — whichever
   instance is free first when the lock opens wins the jittered race.
