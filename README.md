# Text Processing Service
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