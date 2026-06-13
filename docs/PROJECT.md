# Finance Data Stream

The project's goal is to collect finance data from different markets through providers.

The code implements a long-running application in Go.

Supported markets are:

- Forex market via Trading View

These finance data are collected based on user request.

Right now we don't do anything with these data.

# Scope

The application is responsible only for collecting market data.

# Non-goals

The application does not:

- execute trades
- perform analytics
- persist data long-term
