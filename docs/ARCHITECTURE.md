# Architecture

The project is built around some main components and data flow paths. They operate on different layers of abstraction.

## User request

User requests arrived from a web server are treated like a job that should be run and complete.

The job that user intend to run is a data collector getting data from a specific source market based on program configured provider.

## Provider packages

Directories that implement data collection or streaming handlers. They are desirably self-contained and independent packages that share the same input/output structure.

### `tradingview` package

TradingView provider is responsible for:

- establishing connections
- subscribing to symbols
- streaming market data
- normalizing provider-specific messages

## Manager

The manager package coordinates collection jobs.

Responsibilities:

- Receive user requests
- Select the appropriate provider
- Configure provider subscriptions
- Route collected data into pipelines

## `pipeline` package

This package should implement every data pipeline related utility that is going to be used in the whole project.

There is no dependencies to other parts of the project.

# Data Flow

User Request
    ↓
Manager
    ↓
Provider
    ↓
Pipeline

# Ownership

Manager:
- job orchestration

Providers:
- external communication
- data collection

Pipeline:
- data transport and processing