# Distributed Key-Value Store

A fault-tolerant, sharded key-value store built from the ground up. This project implements core distributed systems concepts including consensus (Paxos), replication, sharding, and exactly-once semantics.

## Overview

This repository contains several implementations, each building on the previous:

- **pingpong** - Basic message passing protocol
- **clientserver** - Exactly-once RPC with client request deduplication
- **primarybackup** - View-based replication with automatic failover
- **paxos** - Multi-Paxos consensus with leader election and log compaction
- **shardedstore** - Sharded KV store with dynamic reconfiguration

The final sharded store can handle node failures, network partitions, and dynamic membership changes while maintaining strong consistency guarantees.

## Architecture

The system uses message-passing state machines where nodes communicate asynchronously through messages and timers in single-threaded event loops. This model simplifies reasoning about concurrent behavior and enables systematic model checking.

The framework includes:
- Asynchronous network simulation
- Timer-based event scheduling
- Model checker for state-space exploration
- Visual debugger for execution traces

## Building and Testing

Build the project:
```bash
make
```

Run specific tests:
```bash
./run-tests.py --lab paxos --test testBasic
```

Enable model checking to explore all possible message orderings:
```bash
./run-tests.py --lab paxos --test testBasic --search
```

Additional flags:
- `--timers` - Include timer events in model checking
- `--checks` - Enable additional invariant checks
- `--verbose` - More detailed output

## Key Features

**Paxos Consensus**
- Multi-Paxos with stable leader election
- Heartbeat-based failure detection
- Gap filling with no-ops
- Log compaction and garbage collection
- Liveness through reproposals

**Sharded Store**
- Consistent hashing for shard assignment
- Online shard migration
- Configuration service with its own Paxos group
- Queuing requests during reconfiguration
- Read-only optimization

**Exactly-Once Semantics**
- Client request IDs for deduplication
- Result caching at servers
- Retry logic with exponential backoff

## Project Structure

```
framework/      Core testing and model checking infrastructure
modules/           Individual implementations
  pingpong/     Basic message passing
  clientserver/ RPC with at-most-once semantics
  primarybackup/View-based replication
  paxos/        Multi-Paxos consensus
  shardedstore/ Sharded KV store
```
