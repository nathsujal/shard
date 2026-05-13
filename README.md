# Shard

Shard is a high-performance terminal-based download manager written in Rust.

Built around parallel chunked downloads, resumable transfers, async networking, and efficient disk streaming, Shard aims to provide a fast, reliable, and extensible downloading engine inspired by tools like JDownloader and Free Download Manager — but designed with modern systems programming principles and Rust safety guarantees.

---

# Why "Shard"?

A large file is split into multiple *shards* (chunks), downloaded concurrently, then merged back together.

```text
Large File
    ↓
[Shard 1] [Shard 2] [Shard 3] [Shard 4]
    ↓
Parallel Download Workers
    ↓
Merged Final File
```

---

# Features

* Parallel chunked downloads
* HTTP range request support
* Resumable downloads
* Async concurrent workers
* Retry and recovery mechanisms
* Real-time progress tracking
* Download speed + ETA monitoring
* Efficient streaming-based IO
* Persistent metadata for interrupted downloads
* Linux-first terminal experience
* Extensible architecture for future protocols and integrations

---

# Design Goals

Shard is not intended to be a toy downloader.

Primary focus areas:

* High throughput
* Reliability under unstable networks
* Efficient resource utilization
* Minimal memory overhead
* Clean async architecture
* Strong fault tolerance
* Extensible systems-level design

---

# Architecture Philosophy

Shard treats downloading as a systems engineering problem.

Core ideas:

* Split files into independent ranges
* Download chunks concurrently
* Stream directly to disk
* Persist progress continuously
* Recover safely from interruptions
* Minimize blocking operations
* Keep architecture modular and extensible

---

# Planned Features

## Core

* Multi-part downloads
* Resume support
* Intelligent retries
* Queue management
* Parallel worker scheduling
* Progress visualization

## Advanced

* Interactive terminal dashboard
* Dynamic chunk balancing
* Rate limiting
* Checksum verification
* Browser integration
* BitTorrent support
* Streaming video downloads
* Plugin architecture

---

# Tech Stack

* Rust
* Tokio
* Reqwest
* Async networking
* Streaming filesystem IO

---

# Status

Early development.

Architecture and core download engine currently under active design and implementation.

---

# Vision

Most download managers evolved from legacy architectures over many years.

Shard explores what a modern, async-first, systems-oriented download manager can look like when designed from scratch in Rust.

---

# License

MIT