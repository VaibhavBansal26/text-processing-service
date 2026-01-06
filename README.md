# Text Processing Service

## Demo Screenshots

![1](https://res.cloudinary.com/vaibhav-codexpress/image/upload/v1767662673/Screenshot_2026-01-05_at_8.23.12_PM_tzqw3j.png)


![2](https://res.cloudinary.com/vaibhav-codexpress/image/upload/v1767662673/Screenshot_2026-01-05_at_8.23.26_PM_qz9dpf.png)


## INSTRUCTIONS

Python service that processes and analyzes streaming text data, similar to systems that might handle inputs/outputs for large language models.

Part 1: Core Pipeline (Required)

Build a text processing pipeline with the following components:

1.1 Stream Processor

Create a TextStreamProcessor class that:
* Accepts text input in chunks (simulating streaming data)
* Tokenizes text into words while handling chunk boundaries correctly
* Tracks statistics: word count, unique words, average word length
* Supports multiple concurrent streams with unique stream IDs

1.2 Content Filter

Implement a ContentFilter that:
* Detects and flags potentially problematic content patterns
* Uses configurable rules (e.g., word lists, regex patterns)
* Returns confidence scores for flagged content
* Handles edge cases (partial matches at chunk boundaries)

1.3 Rate Limiter

Create a token-bucket rate limiter that:
* Limits processing rate per stream (e.g., 1000 words/minute)
* Supports different rate limits for different priority levels
* Is thread-safe for concurrent access
* Provides clear feedback when limits are exceeded

Part 2: API Layer (Required)

Build a REST API using FastAPI or Flask with these endpoints:

POST /streams
- Creates a new processing stream
- Returns: stream_id
  
POST /streams/{stream_id}/chunks
- Adds a text chunk to the stream
- Body: {"text": "chunk content", "priority": "normal|high"}
- Returns: processing status
  
GET /streams/{stream_id}/stats
- Returns current statistics for the stream
- Includes: word_count, unique_words, avg_word_length, flags
  
DELETE /streams/{stream_id}
- Closes and cleans up a stream
  
Part 3: Testing (Required)

Provide comprehensive tests, including:
* Unit tests for each component
* Integration tests for the whole pipeline
* Edge case handling (empty chunks, very long words, concurrent access)
* Performance tests demonstrating the system can handle multiple concurrent streams


Part 4: Advanced Features (Choose 2)

Option A: Async Processing

Refactor the system to use asyncio:
* Make all I/O operations async
* Implement backpressure handling
* Support graceful shutdown with cleanup
  
Option B: Persistence Layer

Add data persistence:
* Store stream states to allow recovery after restart
* Use SQLite or a similar lightweight database
* Implement efficient queries for historical statistics
  
Option C: Monitoring & Observability

Add comprehensive monitoring:
* Structured logging with correlation IDs
* Metrics export (Prometheus format or similar)
* Health check endpoint with dependency status
* Request tracing across components
  
Option D: Distributed Processing

Design for horizontal scaling:
* Use Redis or similar for shared state
* Implement distributed rate limiting
* Handle network partitions gracefully
* Document deployment architecture

## Overview

This project is a small, test driven text processing service that supports streaming input and produces live statistics plus content flags. It is designed to run in a simple single process mode by default, and it can be extended into more advanced modes such as persistence, async processing, and distributed processing.

Core outcomes per stream

* word_count
* unique_words
* avg_word_length
* content flags detected by rules
* rate limiting with priority support

Advanced options implemented

* Persistence (SQLite) for restart recovery
* Async processing mode for streaming and backpressure tests
* Observability hooks (basic metrics and tracing middleware)
* Distributed mode (Redis shared state and distributed rate limiting)

## Architecture

The pipeline is the orchestrator. It wires 3 responsibilities together.

* Text Processor: boundary aware token accounting and word statistics
* Content Filter: content scanning rules and flag storage
* Rate limiter: token bucket limiting per stream and priority

In normal mode, all state is in memory.
In persistence mode, state is mirrored to SQLite.
In distributed mode, state is stored in Redis so multiple app instances can share streams.

![3](https://res.cloudinary.com/vaibhav-codexpress/image/upload/v1767665358/Screenshot_2026-01-05_at_9.08.34_PM_kqzd9q.png)

## Trade offs and limitations

### Tokenization model limitations

* The word regex is intentionally simple.
* It may not handle all languages and punctuation edge cases.
* It treats apostrophes as part of tokens, which is good for contractions but can be surprising in some inputs.

### Streaming boundary semantics

* By design, a split word like “wor” then “ld” is not counted as “world” until a delimiter arrives or the stream closes.
* This matches the idea of “completed tokens only”, but it can confuse users who expect immediate counts for partial words.

### Persistence trade offs

* SQLite persistence is easy and test friendly, but it is not ideal for high write throughput.
* Frequent writes can cause lock contention and slowdowns under heavy concurrency.
* Persistence currently focuses on correctness and restart recovery rather than optimal batching.

### Distributed mode limitations

* Redis is a single shared dependency.
* If Redis is unavailable and partition_policy is fail_closed, the system becomes unavailable for distributed operations.
* If partition_policy is fail_open or degraded, correctness guarantees degrade
  * rate limiting becomes approximate
  * state may diverge across instances temporarily

### Network partition handling

What is handled
* Clear policy choices for what to do when Redis is unreachable.

What is not fully solved
* Automatic reconciliation of diverged state in fail_open mode.
* Exactly once semantics across instance restarts.
* Strong ordering guarantees for concurrent writes across instances.

### Rate limiting semantics

* Token bucket uses estimated words per chunk.
* If your estimator differs from actual committed words, the limiter can be slightly conservative or slightly permissive.
* In distributed mode, atomicity is strong, but in fail_open mode, limiting is best effort.

### Security limitations

* The project assumes trusted clients for simplicity.
* Production usage should add auth, request size limits, and more robust validation.
* Redis credentials and TLS should be enforced in production environments.

## Future improvements

* More robust tokenizer options (unicode aware word boundaries)
* Better chunk word accounting so the limiter uses committed words exactly
* Redis health probes and circuit breakers at the API layer

## Time spent on each section
* Core implementation (processor, filter, rate limiter): 3 hours
* Pipeline orchestration and API wiring: 2 hours
* Persistence (SQLite) + recovery tests: 2 hours
* Async processing mode + backpressure: 3 hours
* Distributed mode (Redis) + Lua limiter: 3 hours
* Debugging and making all tests pass: 2 hours
* Documentation and polish: 1 hour



## Installation & Setup

```
git clone

cd text-processing-service

# create python environment

python -m venv .venv

source .venv/bin/activate

# install requirements

pip install -r requirements.txt


```

## Running Test Cases (Instructions)

```
python -m pytest

# Run tests in quiet mode

python -m pytest -q

# Show skipped reasons

python -m pytest -q -rs

# Run distributed mode tests (requires Redis)

export DISTRIBUTED_MODE=1
export REDIS_URL="redis://localhost:6379/0"
python -m pytest tests/test_distributed_mode.py -q -rs

```

## Deployment

```
docker compose --build

# command to start the services

docker compose up -d

# command to stop the services

docker compose down -v


```

## Streamlit Application

```
https://localhost:8501

```