# Text Processing Service

## Demo Screenshots

[1](https://res.cloudinary.com/vaibhav-codexpress/image/upload/v1767662673/Screenshot_2026-01-05_at_8.23.12_PM_tzqw3j.png)

[2](https://res.cloudinary.com/vaibhav-codexpress/image/upload/v1767662673/Screenshot_2026-01-05_at_8.23.26_PM_qz9dpf.png)


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



## Deployment

```
docker compose --build

docker compose up -d

# command to stop the container

docker compose down -v


```