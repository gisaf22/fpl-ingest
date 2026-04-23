# HARD BOUNDARY CONTRACT — FPL INGEST ARCHITECTURE

## PURPOSE

This document defines strict architectural boundaries for the FPL ingestion system.

It exists to prevent:
- logic creep into CLI
- orchestration creep into pipeline
- policy creep into runner
- formatting drift across layers
- hidden coupling between IO and domain logic

This is a structural contract, not documentation.

---

## 1. LAYER RESPONSIBILITIES (ABSOLUTE)

### 1.1 CLI LAYER (`src/fpl_ingest/cli.py`)

#### ALLOWED
- Parse arguments
- Route commands
- Call runner / schema / smoke-test entrypoints
- Print final formatted output (via cli_formatters only)
- Exit with status codes

#### FORBIDDEN
- NO orchestration logic
- NO stage ordering
- NO transaction handling
- NO data transformation
- NO business rules
- NO timing logic
- NO conditional pipeline decisions
- NO SQL or store interaction

#### RULE
CLI is a router only. It does not decide anything.

---

### 1.2 RUNNER LAYER (`pipeline/runner.py`)

#### ALLOWED
- Define execution order of stages
- Coordinate pipeline stages
- Manage transaction boundaries (stage level only)
- Call pipeline functions
- Collect stage results
- Emit structured run-level state

#### FORBIDDEN
- NO business logic inside stages
- NO formatting or printing
- NO schema knowledge
- NO CLI argument parsing
- NO storage implementation details (beyond passing store)
- NO API request logic

#### RULE
Runner is orchestration only. It sequences, it does not interpret.

---

### 1.3 PIPELINE LAYER (`pipeline/*`)

#### ALLOWED
- Implement ingestion logic per domain area:
  - core
  - fixtures
  - gameweeks
  - player histories
- Transform API data → structured records
- Interact with store via defined interfaces
- Emit `StageResult`

#### FORBIDDEN
- NO knowledge of CLI
- NO knowledge of runner
- NO logging format decisions
- NO run-level aggregation
- NO transaction orchestration beyond local stage scope
- NO control flow across stages

#### RULE
Pipeline = execution units only. No orchestration.

---

### 1.4 DOMAIN LAYER (`domain/*`)

#### ALLOWED
- Pure data structures
- Types
- Validation rules
- Run status classification
- Schema definitions

#### FORBIDDEN
- NO IO
- NO logging
- NO orchestration
- NO API calls
- NO persistence logic

#### RULE
Domain must be pure and side-effect free.

---

### 1.5 STORAGE LAYER (`storage/*`)

#### ALLOWED
- SQLite interactions
- Transactions
- Queries
- Persistence of pipeline outputs

#### FORBIDDEN
- NO business logic
- NO transformation logic
- NO pipeline decisions
- NO CLI awareness
- NO stage orchestration

#### RULE
Storage only persists and retrieves.

---

### 1.6 TRANSPORT LAYER (`transport/*`)

#### ALLOWED
- API calls
- Rate limiting
- HTTP client behavior

#### FORBIDDEN
- NO schema interpretation
- NO transformation into domain models beyond mapping
- NO persistence
- NO orchestration

---

## 2. CROSS-CUTTING RULES

### 2.1 Logging Rule
- Logging is allowed in all layers
- Must NOT affect control flow
- Must NOT compute business state
- Must NOT replace domain logic

---

### 2.2 Observability Rule
- Observability is metadata only
- Must NOT influence execution
- Must NOT change pipeline behavior

---

### 2.3 Data Flow Rule

transport → pipeline → storage  
                 ↓  
              domain  
                 ↓  
               runner  
                 ↓  
                CLI  

Reverse direction is forbidden.

---

## 3. HARD ENFORCEMENT CONDITIONS

A violation exists if ANY occur:
- CLI calls pipeline internals directly
- runner contains business logic decisions
- pipeline orchestrates multiple stages
- domain performs IO or logging
- storage determines correctness of data
- transport modifies domain semantics
- formatting exists outside cli_formatters

---

## 4. ARCHITECTURAL INVARIANT

Each layer may only depend on layers BELOW it:

CLI → runner → pipeline → storage/transport → external systems

Domain is shared and dependency-free.

---

## 5. WHY THIS MATTERS

This contract enforces:
- predictable ingestion behavior
- safe future scaling (ML, analytics, dashboards)
- separation of execution vs interpretation
- testability of each layer independently
- prevention of gradual architectural decay