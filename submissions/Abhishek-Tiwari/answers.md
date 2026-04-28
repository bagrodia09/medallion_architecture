## Medallion Architecture - MCQ Answers

**Name:** Abhishek Tiwari
**Date:** 28 April 2026

| Question | Answer | Brief Explanation                                                                          |
| -------- | ------ | ------------------------------------------------------------------------------------------ |
| Q1       | B      | Bronze layer stores raw, unmodified data exactly as received for replayability and audit.  |
| Q2       | C      | Fix logic and reprocess Bronze → Silver to ensure correctness without touching source.     |
| Q3       | C      | Gold should be business-aggregated; copying Silver structure adds no value (anti-pattern). |
| Q4       | B      | Bronze captures schema changes, Silver evolves schema, Gold applies controlled changes.    |
| Q5       | C      | Medallion is for analytics; real-time systems require a separate low-latency hot path.     |
