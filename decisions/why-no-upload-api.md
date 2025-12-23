# Why No Upload API

Decision
- Raw uploads are handled by storage ingestion or external pipeline

Rationale
- Keeps API thin and fast
- Avoids large payloads in service layer
