# Kraken Level 3 Market Data Collector

A high-performance C++ application that collects real-time **Level 3 (full order book)** market data from Kraken cryptocurrency exchange and stores it in QuestDB for analysis. This implementation extends the [crypto-chassis/ccapi](https://github.com/crypto-chassis/ccapi) library with custom Kraken-specific Level 3 order book management.

## Table of Contents

- [Overview](#overview)
- [What is Level 3 Market Data?](#what-is-level-3-market-data)
- [Market Data Collected](#market-data-collected)
  - [Level 3 Order Book Data](#level-3-order-book-data)
  - [Public Trade Data](#public-trade-data)
  - [Derived Market Microstructure Features](#derived-market-microstructure-features)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Build Instructions](#build-instructions)
  - [Prerequisites](#prerequisites)
  - [Building with CMake](#building-with-cmake)
- [Configuration](#configuration)
- [Data Storage](#data-storage)
- [Technical Implementation Details](#technical-implementation-details)
- [Monitored Trading Pairs](#monitored-trading-pairs)
- [Performance Considerations](#performance-considerations)
- [Dependencies](#dependencies)

## Overview

This project implements a real-time market data collection system specifically designed for Kraken's Level 3 order book data. Unlike traditional Level 1 (best bid/ask) or Level 2 (aggregated depth) data, Level 3 provides **individual order-level granularity**, allowing for advanced market microstructure analysis, order flow analysis, and algorithmic trading research.

The collector maintains a full in-memory order book reconstruction with:
- Individual order tracking with unique order IDs
- Price-time priority queue management at each price level
- CRC32 checksum validation for data integrity
- Real-time computation of market microstructure features
- Efficient streaming ingestion to QuestDB time-series database

## What is Level 3 Market Data?

Market data comes in three levels of granularity:

- **Level 1**: Best bid and ask prices with sizes (top of book)
- **Level 2**: Aggregated order book depth showing total size at each price level
- **Level 3**: Full order book showing **every individual order** with unique order IDs, prices, and sizes

Level 3 data provides the most granular view of market structure, enabling:
- Order queue position analysis
- Order flow imbalance (OFI) calculations
- Market maker behavior analysis
- Toxic flow detection
- High-frequency trading research
- Market impact studies

## Market Data Collected

### Level 3 Order Book Data

For each subscribed trading pair, the collector tracks and stores:

#### Individual Order Information
- **Order ID**: Unique identifier for each limit order in the book
- **Price**: Limit price of the order (stored with 7 decimal places precision)
- **Size**: Order quantity (stored with 8 decimal places precision)
- **Side**: Bid or Ask
- **Timestamp**: Arrival time of the order with microsecond precision
- **Queue Position**: Implicit position based on arrival time at each price level

#### Aggregated Level Data (Top 8 Price Levels per Side)
For the best 8 bid and ask price levels, the following aggregated metrics are computed and stored:

- **Price**: The price level (as integer with 10^7 multiplier)
- **Total Size**: Sum of all order sizes at this price level (as integer with 10^8 multiplier)
- **Order Count**: Number of individual orders at this price level

**Data Storage Format**: `kraken_l3_book_levels_agg` table in QuestDB
- Provides a snapshot of the top 8 levels of the order book
- Updates pushed only when the top levels change
- Enables efficient querying of order book state over time

### Public Trade Data

Real-time executed trades are captured with the following fields:

- **Exchange**: "kraken"
- **Asset Pair**: Trading pair symbol (e.g., "BTC/USD")
- **Price**: Execution price
- **Size**: Trade quantity
- **Side**: Taker side ("buy" or "sell")
- **Order Type**: Type of order that caused the trade
- **Timestamp**: Trade execution time with microsecond precision and monotonicity guarantees

**Data Storage Format**: `kraken_trades` table in QuestDB

### Derived Market Microstructure Features

For the top 5 price levels on each side, advanced features are calculated in real-time:

#### Order Queue Features (per level)
- **orders80pct**: Number of orders needed to represent 80% of total volume at the level
- **HHI** (Herfindahl-Hirschman Index): Measure of order size concentration (sum of squared market shares)
- **topOrderSize**: Size of the order at the front of the queue
- **topOrderAge_ms**: Age in milliseconds of the order at the front of the queue

#### Order Flow Imbalance (OFI)
- **ofi_level1 through ofi_level5**: Change in volume at each of the top 5 price levels
- Calculated as: `current_volume - previous_volume` at each price level
- Separate calculations for bid and ask sides
- Useful for predicting short-term price movements

**Data Storage Format**: `kraken_l3_book_level_features` and `kraken_ofi` tables in QuestDB

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kraken Exchange                              │
│  ┌──────────────────┐         ┌──────────────────┐            │
│  │  Authenticated   │         │   Public WS      │            │
│  │  WebSocket API   │         │   Trade Feed     │            │
│  │   (Level 3)      │         │                  │            │
│  └────────┬─────────┘         └────────┬─────────┘            │
└───────────┼──────────────────────────────┼────────────────────┘
            │                              │
            │ Order book events            │ Trade events
            │ (add/modify/delete)          │
            ▼                              ▼
    ┌───────────────────────────────────────────────────┐
    │         CCAPI Session (WebSocket)                 │
    │  - Connection management                          │
    │  - Authentication & token refresh                 │
    │  - Message parsing                                │
    └───────────────────┬───────────────────────────────┘
                        │
                        ▼
    ┌────────────────────────────────────────────────────┐
    │          MyEventHandler                            │
    │  - Per-instrument state management                 │
    │  - Order book reconstruction                       │
    │  - CRC32 checksum validation                       │
    │  - Feature calculation                             │
    └────────────────────┬───────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────┐
    │       ThreadSafeQueue<QdbEvent>                    │
    │  - Decouples data collection from ingestion        │
    │  - Lock-free queue for high throughput             │
    └────────────────────┬───────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────┐
    │      QuestDB Writer Thread                         │
    │  - Batched writes via ILP (line protocol)          │
    │  - Automatic table switching                       │
    │  - Error handling & retry logic                    │
    └────────────────────┬───────────────────────────────┘
                         │
                         ▼
    ┌────────────────────────────────────────────────────┐
    │              QuestDB Database                      │
    │  - Time-series optimized storage                   │
    │  - Fast columnar queries                           │
    │  - Native timestamp indexing                       │
    └────────────────────────────────────────────────────┘
```

## Key Features

### Order Book Management
- **Full Level 3 reconstruction**: Maintains complete order book state with individual orders
- **Price-time priority**: Orders at each price level stored in arrival order (FIFO queue)
- **Efficient updates**: Uses hash maps for O(1) order lookup and linked lists for queue management
- **Memory bounded**: Automatically prunes orders beyond configured depth (10 levels by default)
- **CRC32 validation**: Validates order book integrity using Kraken-provided checksums

### Data Integrity
- **Checksum verification**: Every order book update is validated against CRC32 checksums
- **Deferred verification**: Intelligent deferral system to handle transient inconsistencies
- **Automatic resubscription**: Triggers resubscription on persistent checksum failures
- **Monotonic timestamps**: Ensures all trades have strictly increasing timestamps

### High Performance
- **Lock-free data structures**: Thread-safe queues minimize contention
- **Fixed-point arithmetic**: Uses integer representation (price × 10^7, size × 10^8) for precision
- **Zero-copy design**: Minimizes string allocations and copying
- **Batched database writes**: Aggregates multiple updates before flushing to QuestDB
- **Multi-threaded**: Separate threads for data collection and database ingestion

### Production Ready
- **Robust error handling**: Graceful handling of connection issues, malformed data
- **Comprehensive logging**: Detailed debug logs with configurable verbosity
- **Graceful shutdown**: Clean resource cleanup on termination
- **Configuration via environment**: API credentials passed through environment variables

## Build Instructions

### Prerequisites

- **C++17 compatible compiler**: GCC 7+, Clang 5+, or MSVC 2017+
- **CMake**: Version 3.20 or higher
- **vcpkg**: For dependency management
- **OpenSSL**: For secure WebSocket connections
- **Boost**: Version 1.87.0 or higher (required by CCAPI)
- **ZLIB**: For data compression
- **QuestDB**: Version 7.0+ running locally or remotely

Required vcpkg packages:
```bash
vcpkg install boost openssl nlohmann-json rapidjson zlib
```

### Building with CMake

1. **Clone the repository** (or navigate to project directory):
```bash
cd c:\dev\ccapi
```

2. **Configure environment variables** for Kraken API credentials:
```bash
# Windows (PowerShell)
$env:KRAKEN_API_KEY = "your-api-key"
$env:KRAKEN_API_SECRET = "your-api-secret"

# Linux/macOS
export KRAKEN_API_KEY="your-api-key"
export KRAKEN_API_SECRET="your-api-secret"
```

3. **Create build directory and configure**:
```bash
mkdir build
cd build
cmake .. -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake
```

4. **Build the project**:
```bash
cmake --build . --config Release
```

5. **Run the collector**:
```bash
# Run for 60 seconds (default)
./kraken_l3_collector

# Run for custom duration (e.g., 300 seconds)
./kraken_l3_collector 300
```

## Configuration

### Environment Variables

The collector requires Kraken API credentials to access authenticated Level 3 data:

- `KRAKEN_API_KEY`: Your Kraken API public key
- `KRAKEN_API_SECRET`: Your Kraken API secret key

**Note**: The API key must have permissions for:
- Query Funds
- Query Open Orders & Trades
- WebSocket Authentication

### QuestDB Configuration

By default, the collector connects to QuestDB at:
- **Host**: `localhost`
- **Port**: `9009` (ILP/TCP)

To modify, edit the constants in [kraken_l3_collector.cpp](example/src/kraken_l3_collector.cpp):
```cpp
const std::string QUESTDB_HOST = "localhost";
const int QUESTDB_ILP_TCP_PORT = 9009;
```

### Depth Configuration

The collector can be configured for different order book depths:

```cpp
const size_t MAX_BOOK_DEPTH = 10;              // Maximum levels to maintain
const int NUM_AGGREGATED_LEVELS_TO_SEND = 8;  // Levels to send to QuestDB
const int CRC_DEPTH = 10;                      // Levels used for checksum
```

## Data Storage

### QuestDB Tables

The collector creates and populates the following tables:

#### 1. `kraken_l3_book_levels_agg`
Top-of-book aggregated levels (8 levels per side).

**Columns**:
- `timestamp` (TIMESTAMP)
- `symbol` (SYMBOL)
- `side` (SYMBOL): "bid" or "ask"
- `level_idx` (INT): 0-7 representing top 8 levels
- `price_long` (LONG): Price × 10^7
- `size_long` (LONG): Total size × 10^8
- `num_orders` (LONG): Count of orders at level

#### 2. `kraken_trades`
Executed trades in real-time.

**Columns**:
- `timestamp` (TIMESTAMP)
- `symbol` (SYMBOL)
- `price` (DOUBLE)
- `size` (DOUBLE)
- `side` (SYMBOL): "buy" or "sell"
- `ord_type` (STRING)

#### 3. `kraken_l3_book_level_features`
Advanced microstructure features for top 5 levels.

**Columns**:
- `timestamp` (TIMESTAMP)
- `symbol` (SYMBOL)
- `side` (SYMBOL)
- `level_idx` (INT): 0-4
- `orders_80pct` (LONG)
- `hhi` (DOUBLE)
- `top_order_size` (DOUBLE)
- `top_order_age_ms` (LONG)

#### 4. `kraken_ofi`
Order Flow Imbalance metrics.

**Columns**:
- `timestamp` (TIMESTAMP)
- `symbol` (SYMBOL)
- `ofi_level1_bid` through `ofi_level5_bid` (DOUBLE)
- `ofi_level1_ask` through `ofi_level5_ask` (DOUBLE)

## Technical Implementation Details

### Precision and Fixed-Point Arithmetic

To avoid floating-point precision issues in financial calculations:

- **Prices**: Stored as `int64_t` with 7 decimal places (`price_long = price × 10^7`)
- **Sizes**: Stored as `int64_t` with 8 decimal places (`size_long = size × 10^8`)

This ensures exact representation and fast integer arithmetic while maintaining sufficient precision for cryptocurrency markets.

### Order Book Data Structure

```cpp
struct L3PriceLevel {
  int64_t price_l;                                      // Fixed-point price
  std::string price;                                    // Original price string
  std::list<L3Order> orders;                            // FIFO queue of orders
  std::map<std::string, std::list<L3Order>::iterator> orderId_to_iter;
  int64_t totalSizeAtLevel_l;                          // Cached sum
};
```

**Bid Book**: `std::map<int64_t, L3PriceLevel, std::greater<int64_t>>` (descending order)
**Ask Book**: `std::map<int64_t, L3PriceLevel>` (ascending order)

### CRC32 Checksum Validation

Kraken provides CRC32 checksums with every update. The collector:
1. Computes a local checksum from the top 10 levels
2. Compares against the exchange-provided checksum
3. Defers verification briefly on mismatches (to handle message ordering issues)
4. Triggers resubscription if persistent mismatches detected

### Event Types Handled

From Kraken's WebSocket API:
- `add`: New order added to book
- `modify`: Existing order size changed (price unchanged)
- `delete`: Order removed from book
- `snapshot`: Full book state (on initial subscription)

## Monitored Trading Pairs

The collector currently monitors these 10 major cryptocurrency pairs on Kraken:

1. BTC/USD (Bitcoin)
2. ETH/USD (Ethereum)
3. USDT/USD (Tether)
4. SOL/USD (Solana)
5. XRP/USD (Ripple)
6. DOGE/USD (Dogecoin)
7. ADA/USD (Cardano)
8. LTC/USD (Litecoin)
9. LINK/USD (Chainlink)
10. DOT/USD (Polkadot)

To modify the list, edit the `PAIRS_TO_SUBSCRIBE` vector in [kraken_l3_collector.cpp](example/src/kraken_l3_collector.cpp):
```cpp
const std::vector<std::string> PAIRS_TO_SUBSCRIBE = {
  "BTC/USD", "ETH/USD", /* add more pairs */
};
```

## Performance Considerations

### Throughput
- Handles **1000+ order book updates per second** per trading pair
- Batched database writes reduce I/O overhead
- Lock-free queue enables producer/consumer parallelism

### Latency
- **Sub-millisecond** in-memory order book updates
- **10-50ms** end-to-end latency from exchange to QuestDB (depends on network)

### Memory Usage
- Approximately **50-100 MB** per trading pair with 10-level depth
- Automatic pruning keeps memory bounded

### CPU Usage
- **Single-threaded** event processing for each instrument (ensures ordering)
- **Separate thread** for QuestDB ingestion
- Modern multi-core CPUs can easily handle 10+ pairs simultaneously

## Dependencies

### Core Libraries
- **[ccapi](https://github.com/crypto-chassis/ccapi)**: Cryptocurrency exchange API library (header-only)
- **Boost 1.87.0+**: For ASIO (async I/O), regex, and other utilities
- **OpenSSL**: For WebSocket TLS connections
- **RapidJSON**: Fast JSON parsing (used by ccapi)
- **nlohmann/json**: Modern C++ JSON library (used for some parsing)
- **ZLIB**: Data compression support

### Database
- **[QuestDB C++ Client](https://github.com/questdb/c-questdb-client)**: Ingestion library (ILP protocol)
  - Automatically fetched via CMake FetchContent
  - Version 4.0.5

### Build Tools
- **CMake 3.20+**: Build system
- **vcpkg**: Package manager for C++ dependencies

---

## Original CCAPI Library

This project builds upon the excellent [crypto-chassis/ccapi](https://github.com/crypto-chassis/ccapi) library, which provides a unified API for multiple cryptocurrency exchanges. The original library supports:

- **60+ exchanges** for market data and execution management
- REST and WebSocket APIs
- FIX protocol support
- Multi-language bindings (Python, Java, C#, Go, JavaScript)

For generic usage of the CCAPI library (non-Kraken Level 3 specific), please refer to the original documentation below or visit [the official repository](https://github.com/crypto-chassis/ccapi).

---

## License

This project inherits the license from the [crypto-chassis/ccapi](https://github.com/crypto-chassis/ccapi) library. Please refer to the LICENSE file in the repository.

## Support

For issues specific to the Kraken Level 3 implementation, please open an issue in this repository.

For general CCAPI questions, visit the [CCAPI Discord](https://discord.gg/b5EKcp9s8T) or [Medium](https://cryptochassis.medium.com).

---

## Acknowledgments

- **crypto-chassis**: For the excellent CCAPI library
- **Kraken**: For providing robust Level 3 market data APIs
- **QuestDB**: For the high-performance time-series database

---

# Original CCAPI Documentation

Below is the original README content for the generic CCAPI library:

---

# Some breaking changes introduced
* Please update boost version to at least 1.87.0.
* When a subscription fails due to the underlying websocket connection fails to open, the emitted message type is SUBSCRIPTION_FAILURE_DUE_TO_CONNECTION_FAILURE instead of SUBSCRIPTION_FAILURE.
* Removed the spot market making application and the single order execution application.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ccapi](#ccapi)
  - [Branches](#branches)
  - [Build](#build)
    - [C++](#c)
    - [non-C++](#non-c)
  - [Constants](#constants)
  - [Examples](#examples)
  - [Documentations](#documentations)
    - [Simple Market Data](#simple-market-data)
    - [Advanced Market Data](#advanced-market-data)
      - [Complex request parameters](#complex-request-parameters)
      - [Specify subscription market depth](#specify-subscription-market-depth)
      - [Specify correlation id](#specify-correlation-id)
      - [Multiple exchanges and/or instruments](#multiple-exchanges-andor-instruments)
      - [Receive subscription events at periodic intervals](#receive-subscription-events-at-periodic-intervals)
      - [Receive subscription events at periodic intervals including when the market depth snapshot hasn't changed](#receive-subscription-events-at-periodic-intervals-including-when-the-market-depth-snapshot-hasnt-changed)
      - [Receive subscription market depth updates](#receive-subscription-market-depth-updates)
      - [Receive subscription trade events](#receive-subscription-trade-events)
      - [Receive subscription calculated-candlestick events at periodic intervals](#receive-subscription-calculated-candlestick-events-at-periodic-intervals)
      - [Receive subscription exchange-provided-candlestick events at periodic intervals](#receive-subscription-exchange-provided-candlestick-events-at-periodic-intervals)
      - [Send generic public requests](#send-generic-public-requests)
      - [Make generic public subscriptions](#make-generic-public-subscriptions)
      - [Send generic private requests](#send-generic-private-requests)
    - [Simple Execution Management](#simple-execution-management)
    - [Advanced Execution Management](#advanced-execution-management)
      - [Specify correlation id](#specify-correlation-id-1)
      - [Multiple exchanges and/or instruments](#multiple-exchanges-andor-instruments-1)
      - [Multiple subscription fields](#multiple-subscription-fields)
      - [Make Session::sendRequest blocking](#make-sessionsendrequest-blocking)
      - [Provide API credentials for an exchange](#provide-api-credentials-for-an-exchange)
      - [Override exchange urls](#override-exchange-urls)
      - [Complex request parameters](#complex-request-parameters-1)
      - [Send request by Websocket API](#send-request-by-websocket-api)
      - [Specify instrument type](#specify-instrument-type)
    - [FIX API](#fix-api)
    - [More Advanced Topics](#more-advanced-topics)
      - [Handle events in "immediate" vs. "batching" mode](#handle-events-in-immediate-vs-batching-mode)
      - [Thread safety](#thread-safety)
      - [Enable library logging](#enable-library-logging)
      - [Set timer](#set-timer)
  - [Performance Tuning](#performance-tuning)
  - [Known Issues and Workarounds](#known-issues-and-workarounds)
  - [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
# ccapi
* A header-only C++ library for streaming market data and executing trades directly from cryptocurrency exchanges (i.e. the connections are between your server and the exchange server without anything in-between).
* Bindings for other languages such as Python, Java, C#, Go, and Javascript are provided.
* Code closely follows Bloomberg's API: https://www.bloomberg.com/professional/support/api-library/.
* It is ultra fast thanks to very careful optimizations: move semantics, regex optimization, locality of reference, lock contention minimization, etc.
* Supported exchanges:
  * Market Data: ascendex, binance, binanceds-futures, binance-coin-futures, binance, bitfinex, bitget, bitget-futures, bitmart, bitmex, bitstamp, bybit, okx, cryptocom, deribit, erisx (Cboe Digital), gateio, gateio-perpetual-futures, gemini, huobi, huobi-usdt-swap, huobi-coin-swap, kraken, kraken-futures, kucoin, kucoin-futures, mexc, mexc-futures, okx, whitebit.
  * Execution Management: ascendex, binance, binanceds-futures, binance-coin-futures, binance, bitfinex, bitget, bitget-futures, bitmart, bitmex, bitstamp, bybit, okx, cryptocom, deribit, erisx (Cboe Digital), gateio, gateio-perpetual-futures, gemini, huobi, huobi-usdt-swap, huobi-coin-swap, kraken, kraken-futures, kucoin, kucoin-futures, mexc, okx.
  * FIX: okx, gemini.
* Join us on Discord https://discord.gg/b5EKcp9s8T and Medium https://cryptochassis.medium.com.

## Branches
* The `develop` branch may contain experimental features.
* The `master` branch represents the most recent stable release.

## Build

### C++
* This library is header-only.
* Example CMake: example/CMakeLists.txt.
* Require C++17 and OpenSSL.
* Macros in the compiler command line:
  * Define service enablement macro such as `CCAPI_ENABLE_SERVICE_MARKET_DATA`, `CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT`, `CCAPI_ENABLE_SERVICE_FIX`, etc. and exchange enablement macros such as `CCAPI_ENABLE_EXCHANGE_OKX`, etc. These macros can be found at the top of [`include/ccapi_cpp/ccapi_session.h`](include/ccapi_cpp/ccapi_session.h).
* Dependencies:
  * boost https://archives.boost.io/release/1.87.0/source/boost_1_87_0.tar.gz (notice that its include directory is boost).
  * rapidjson https://github.com/Tencent/rapidjson/archive/refs/tags/v1.1.0.tar.gz (notice that its include directory is rapidjson/include).
  * If you use FIX API, also need hffix https://github.com/jamesdbrock/hffix/archive/refs/tags/v1.4.1.tar.gz (notice that its include directory is hffix/include).
* Include directory for this library:
  * include.
* Link libraries:
  * OpenSSL: libssl.
  * OpenSSL: libcrypto.
  * If you need market data for huobi/huobi-usdt-swap/huobi-coin-swap or execution management for huobi-usdt-swap/huobi-coin-swap/bitmart, also link ZLIB.
  * On Windows, also link ws2_32.
* Compiler flags:
  * `-pthread` for GCC and MinGW.
* Tested platforms:
  * macOS: Clang.
  * Linux: GCC.
  * Windows: MinGW.
* Troubleshoot:
  * Try to remove all build artifacts and start from scratch (e.g. for cmake remove all the contents inside your build directory).
  * "Could NOT find OpenSSL, try to set the path to OpenSSL root folder in the system variable OPENSSL_ROOT_DIR (missing: OPENSSL_INCLUDE_DIR)". Try `cmake -DOPENSSL_ROOT_DIR=...`. On macOS, you might be missing headers for OpenSSL, `brew install openssl` and `cmake -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl`. On Ubuntu, `sudo apt-get install libssl-dev`. On Windows, `vcpkg install openssl:x64-windows` and `cmake -DOPENSSL_ROOT_DIR=C:/vcpkg/installed/x64-windows-static`.
  * "Fatal error: can't write \<a> bytes to section .text of \<b>: 'File too big'". Try to add compiler flag `-Wa,-mbig-obj`. See https://github.com/assimp/assimp/issues/2067.
  * "string table overflow at offset \<a>". Try to add optimization flag `-O1` or `-O2`. See https://stackoverflow.com/questions/14125007/gcc-string-table-overflow-error-during-compilation.
  * On Windows, if you still encounter resource related issues, try to add optimization flag `-O3 -DNDEBUG`.

### non-C++
* Require SWIG and CMake.
  * SWIG: On macOS, `brew install SWIG`. On Linux, `sudo apt-get install -y swig`.
  * CMake: https://cmake.org/download/.
* Run the following commands.
```
mkdir binding/build
cd binding/build
rm -rf * (if rebuild from scratch)
cmake -DBUILD_PYTHON=ON -DBUILD_VERSION=1.0.0 .. (Use -DBUILD_JAVA=ON if the target language is Java, -DBUILD_CSHARP=ON if the target language is C#, -DBUILD_GO=ON if the target language is Go, -DBUILD_JAVASCRIPT=ON if the target language is Javascript)
cmake --build .
```
* The packaged build artifacts are located in the `binding/build/<language>/packaging/<BUILD_VERSION>` directory. SWIG generated raw files and build artifacts are located in the `binding/build/<language>/ccapi_binding_<language>` directory.
* Python: If a virtual environment (managed by `venv` or `conda`) is active (i.e. the `activate` script has been evaluated), the package will be installed into the virtual environment rather than globally.
* C#: The shared library is built using the .NET framework.
* Troubleshoot:
  * "Could NOT find OpenSSL, try to set the path to OpenSSL root folder in the system variable OPENSSL_ROOT_DIR (missing: OPENSSL_INCLUDE_DIR)". Try `cmake -DOPENSSL_ROOT_DIR=...`. On macOS, you might be missing headers for OpenSSL, `brew install openssl` and `cmake -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl`. On Ubuntu, `sudo apt-get install libssl-dev`. On Windows, `vcpkg install openssl:x64-windows` and `cmake -DOPENSSL_ROOT_DIR=C:/vcpkg/installed/x64-windows-static`.
  * Python:
    * "CMake Error at python/CMakeLists.txt:... (message): Require Python 3". Try to create and activate a virtual environment (managed by `venv` or `conda`) with Python 3.
    * "'_PyObject_GC_UNTRACK' was not declared in this scope". If you use Python >= 3.8, please use SWIG >= 4.0.
  * Java:
    * "Could NOT find JNI (missing: JAVA_INCLUDE_PATH JAVA_INCLUDE_PATH2 JAVA_AWT_INCLUDE_PATH)". Check that the environment variable `JAVA_HOME` is correct.
  * Javascript:
    * "Check for node-gyp Program: not found". You can install node-gyp using npm: `npm install -g node-gyp`

## Constants
[`include/ccapi_cpp/ccapi_macro.h`](include/ccapi_cpp/ccapi_macro.h)

## Examples
[C++](example)
* Require CMake.
  * CMake: https://cmake.org/download/.
* Run the following commands.
```
mkdir example/build
cd example/build
rm -rf * (if rebuild from scratch)
cmake ..
cmake --build . --target <example-name>
```
* The executable is `example/build/src/<example-name>/<example-name>`. Run it.

[Python](binding/python/example)
* Python API is nearly identical to C++ API and covers nearly all the functionalities from C++ API.
* Build and install the Python binding as shown [above](#non-c).
* Inside a concrete example directory (e.g. binding/python/example/market_data_simple_subscription), run
```
python3 main.py
```
* Troubleshoot:
  * "Fatal Python error: Segmentation fault". If the macOS version is relatively new and the Python version is relatively old, please upgrade Python to a relatively new version.

[Java](binding/java/example)
* Java API is nearly identical to C++ API and covers nearly all the functionalities from C++ API.
* Build and install the Java binding as shown [above](#non-c).
* Inside a concrete example directory (e.g. binding/python/example/market_data_simple_subscription), run
```
mkdir build
cd build
rm -rf * (if rebuild from scratch)
javac -cp ../../../../build/java/packaging/1.0.0/ccapi-1.0.0.jar -d . ../Main.java
java -cp .:../../../../build/java/packaging/1.0.0/ccapi-1.0.0.jar -Djava.library.path=../../../../build/java/packaging/1.0.0  Main
```
* Troubleshoot:
  * "../Main.java:1: error: package com.cryptochassis.ccapi does not exist". Check that `javac`'s classpath includes `binding/build/java/packaging/1.0.0/ccapi-1.0.0.jar`.
  * "Exception in thread "main" java.lang.UnsatisfiedLinkError: no ccapi_binding_java in java.library.path: ...". Check that `java`'s `java.library.path` property includes `binding/build/java/packaging/1.0.0`. See https://stackoverflow.com/questions/1403788/java-lang-unsatisfiedlinkerror-no-dll-in-java-library-path.

[C#](binding/csharp/example)
* C# API is nearly identical to C++ API and covers nearly all the functionalities from C++ API.
* Build and install the C# binding as shown [above](#non-c).
* Inside a concrete example directory (e.g. binding/csharp/example/market_data_simple_subscription), run
```
dotnet clean (if rebuild from scratch)
env LD_LIBRARY_PATH="$LD_LIBRARY_PATH:../../../build/csharp/packaging/1.0.0" dotnet run --property:CcapiLibraryPath=../../../build/csharp/packaging/1.0.0/ccapi.dll -c Release
```
* Troubleshoot:
  * "error CS0246: The type or namespace name 'ccapi' could not be found". Check that you aren't missing the ccapi assembly reference.
  * "System.DllNotFoundException: Unable to load shared library 'ccapi_binding_csharp.so' or one of its dependencies.". Check that environment variable `LD_LIBRARY_PATH` includes `binding/build/csharp/packaging/1.0.0`.

[Go](binding/go/example)
* Go API is nearly identical to C++ API and covers nearly all the functionalities from C++ API.
* Build and install the Go binding as shown [above](#non-c).
* Inside a concrete example directory (e.g. binding/go/example/market_data_simple_subscription), run
```
go clean (if rebuild from scratch)
source ../../../build/go/packaging/1.0.0/export_compiler_options.sh (this step is important)
go build .
./main
```
* Troubleshoot:
  * Some C/C++ header files not found. Check that you sourced the export_compiler_options.sh file which provides important environment variables needed by the cgo tool.

[Javascript](binding/javascript/example)
* Javascript API is nearly identical to C++ API and covers nearly all the functionalities from C++ API.
* Build and install the Javascript binding as shown [above](#non-c).
* Inside a concrete example directory (e.g. binding/javascript/example/market_data_simple_subscription), run
```
rm -rf node_modules (if rebuild from scratch)
npm install
node index.js
```

## Documentations

### Simple Market Data

**Objective 1:**

For a specific exchange and instrument, get recents trades.

**Code 1:**

[C++](example/src/market_data_simple_request/main.cpp) / [Python](binding/python/example/market_data_simple_request/main.py) / [Java](binding/java/example/market_data_simple_request/Main.java) / [C#](binding/csharp/example/market_data_simple_request/MainProgram.cs) / [Go](binding/go/example/market_data_simple_request/main.go) / [Javascript](binding/javascript/example/market_data_simple_request/index.js)
```
#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;  // This line is needed.

class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    std::cout << "Received an event:\n" + event.toStringPretty(2, 2) << std::endl;
    return true;
  }
};
} /* namespace ccapi */

using ::ccapi::MyEventHandler;
using ::ccapi::Request;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;

int main(int argc, char** argv) {
  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;
  MyEventHandler eventHandler;
  Session session(sessionOptions, sessionConfigs, &eventHandler);
  Request request(Request::Operation::GET_RECENT_TRADES, "okx", "BTC-USDT");
  request.appendParam({
      {"LIMIT", "1"},
  });
  session.sendRequest(request);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  session.stop();
  std::cout << "Bye" << std::endl;
  return EXIT_SUCCESS;
}

```

**Output 1:**
```console
Received an event:
  Event [
    type = RESPONSE,
    messageList = [
      Message [
        type = GET_RECENT_TRADES,
        recapType = UNKNOWN,
        time = 2021-05-25T03:23:31.124000000Z,
        timeReceived = 2021-05-25T03:23:31.239734000Z,
        elementList = [
          Element [
            nameValueMap = {
              IS_BUYER_MAKER = 1,
              LAST_PRICE = 38270.71,
              LAST_SIZE = 0.001,
              TRADE_ID = 178766798
            }
          ]
        ],
        correlationIdList = [ 5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE ]
      ]
    ]
  ]
Bye
```
* Request operation types: `GET_SERVER_TIME`, `GET_INSTRUMENT`, `GET_INSTRUMENTS`, `GET_BBOS`, `GET_RECENT_TRADES`, `GET_HISTORICAL_TRADES`, `GET_RECENT_CANDLESTICKS`, `GET_HISTORICAL_CANDLESTICKS`, `GET_RECENT_AGG_TRADES`, `GET_HISTORICAL_AGG_TRADES`(only applicable to binance family: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list), ``.
* Request parameter names: `LIMIT`, `INSTRUMENT_TYPE`, `CANDLESTICK_INTERVAL_SECONDS`, `START_TIME_SECONDS`, `END_TIME_SECONDS`, `START_TRADE_ID`, `END_TRADE_ID`, `START_AGG_TRADE_ID`, `END_AGG_TRADE_ID`. Instead of these convenient names you can also choose to use arbitrary parameter names and they will be passed to the exchange's native API. See [this example](example/src/market_data_advanced_request/main.cpp).
* Message's `time` represents the exchange's reported timestamp. Its `timeReceived` represents the library's receiving timestamp. `time` can be retrieved by `getTime` method and `timeReceived` can be retrieved by `getTimeReceived` method. (For non-C++, please use `getTimeUnix` and `getTimeReceivedUnix` methods or `getTimeISO` and `getTimeReceivedISO` methods).

**Objective 2:**

For a specific exchange and instrument, whenever the best bid's or ask's price or size changes, print the market depth snapshot at that moment.

**Code 2:**

[C++](example/src/market_data_simple_subscription/main.cpp) / [Python](binding/python/example/market_data_simple_subscription/main.py) / [Java](binding/java/example/market_data_simple_subscription/Main.java) / [C#](binding/csharp/example/market_data_simple_subscription/MainProgram.cs) / [Go](binding/go/example/market_data_simple_subscription/main.go) / [Javascript](binding/javascript/example/market_data_simple_subscription/index.js)
```
#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;  // This line is needed.

class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    if (event.getType() == Event::Type::SUBSCRIPTION_STATUS) {
      std::cout << "Received an event of type SUBSCRIPTION_STATUS:\n" + event.toStringPretty(2, 2) << std::endl;
    } else if (event.getType() == Event::Type::SUBSCRIPTION_DATA) {
      for (const auto& message : event.getMessageList()) {
        std::cout << std::string("Best bid and ask at ") + UtilTime::getISOTimestamp(message.getTime()) + " are:" << std::endl;
        for (const auto& element : message.getElementList()) {
          const std::map<std::string, std::string>& elementNameValueMap = element.getNameValueMap();
          std::cout << "  " + toString(elementNameValueMap) << std::endl;
        }
      }
    }
    return true;
  }
};
} /* namespace ccapi */

using ::ccapi::MyEventHandler;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;
using ::ccapi::Subscription;
using ::ccapi::toString;

int main(int argc, char** argv) {
  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;
  MyEventHandler eventHandler;
  Session session(sessionOptions, sessionConfigs, &eventHandler);
  Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH");
  session.subscribe(subscription);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  session.stop();
  std::cout << "Bye" << std::endl;
  return EXIT_SUCCESS;
}

```

**Output 2:**
```console
Best bid and ask at 2020-07-27T23:56:51.884855000Z are:
  {BID_PRICE=10995, BID_SIZE=0.22187803}
  {ASK_PRICE=10995.44, ASK_SIZE=2}
Best bid and ask at 2020-07-27T23:56:51.935993000Z are:
  ...
```
* Subscription fields: `MARKET_DEPTH`, `TRADE`, `CANDLESTICK`, `AGG_TRADE`(only applicable to binance family: https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams).

### Advanced Market Data

#### Complex request parameters
Please follow the exchange's API documentations: e.g. https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-trades-history.
```
Request request(Request::Operation::GET_HISTORICAL_TRADES, "okx", "BTC-USDT");
request.appendParam({
  {"before", "1"},
  {"after", "3"},
  {"limit", "1"},
});
```

#### Specify subscription market depth

Instantiate `Subscription` with option `MARKET_DEPTH_MAX` set to be the desired market depth (e.g. you want to receive market depth snapshot whenever the top 10 bid's or ask's price or size changes).
```
Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH", "MARKET_DEPTH_MAX=10");
```

#### Specify correlation id

Instantiate `Request` with the desired correlationId. The `correlationId` should be unique.
```
Request request(Request::Operation::GET_RECENT_TRADES, "okx", "BTC-USDT", "cool correlation id");
```
Instantiate `Subscription` with the desired correlationId.
```
Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH", "", "cool correlation id");
```
This is used to match a particular request or subscription with its returned data. Within each `Message` there is a `correlationIdList` to identify the request or subscription that requested the data.

#### Multiple exchanges and/or instruments

Send a `std::vector<Request>`.
```
Request request_1(Request::Operation::GET_RECENT_TRADES, "okx", "BTC-USDT", "cool correlation id for BTC");
request_1.appendParam(...);
Request request_2(Request::Operation::GET_RECENT_TRADES, "binance", "ETH-USDT", "cool correlation id for ETH");
request_2.appendParam(...);
session.sendRequest({request_1, request_2});
```
Subscribe a `std::vector<Subscription>`.
```
Subscription subscription_1("okx", "BTC-USDT", "MARKET_DEPTH", "", "cool correlation id for okx BTC-USDT");
Subscription subscription_2("binance", "ETH-USDT", "MARKET_DEPTH", "", "cool correlation id for binance ETH-USDT");
session.subscribe({subscription_1, subscription_2});
```

#### Receive subscription events at periodic intervals

Instantiate `Subscription` with option `CONFLATE_INTERVAL_MILLISECONDS` set to be the desired interval.
```
Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH", "CONFLATE_INTERVAL_MILLISECONDS=1000");
```

#### Receive subscription events at periodic intervals including when the market depth snapshot hasn't changed

Instantiate `Subscription` with option `CONFLATE_INTERVAL_MILLISECONDS` set to be the desired interval and `CONFLATE_GRACE_PERIOD_MILLISECONDS` to be the grace period for late events.
```
Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH", "CONFLATE_INTERVAL_MILLISECONDS=1000&CONFLATE_GRACE_PERIOD_MILLISECONDS=0");
```

#### Receive subscription market depth updates

Instantiate `Subscription` with option `MARKET_DEPTH_RETURN_UPDATE` set to 1. This will return the order book updates instead of snapshots.
```
Subscription subscription("okx", "BTC-USDT", "MARKET_DEPTH", "MARKET_DEPTH_RETURN_UPDATE=1&MARKET_DEPTH_MAX=2");
```

#### Receive subscription trade events

Instantiate `Subscription` with field `TRADE`.
```
Subscription subscription("okx", "BTC-USDT", "TRADE");
```

#### Receive subscription calculated-candlestick events at periodic intervals

Instantiate `Subscription` with field `TRADE` and option `CONFLATE_INTERVAL_MILLISECONDS` set to be the desired interval and `CONFLATE_GRACE_PERIOD_MILLISECONDS` to be your network latency.
```
Subscription subscription("okx", "BTC-USDT", "TRADE", "CONFLATE_INTERVAL_MILLISECONDS=5000&CONFLATE_GRACE_PERIOD_MILLISECONDS=0");
```

#### Receive subscription exchange-provided-candlestick events at periodic intervals

Instantiate `Subscription` with field `CANDLESTICK` and option `CANDLESTICK_INTERVAL_SECONDS` set to be the desired interval.
```
Subscription subscription("okx", "BTC-USDTT", "CANDLESTICK", "CANDLESTICK_INTERVAL_SECONDS=60");
```

#### Send generic public requests

Instantiate `Request` with operation `GENERIC_PUBLIC_REQUEST`. Provide request parameters `HTTP_METHOD`, `HTTP_PATH`, and optionally `HTTP_QUERY_STRING` (query string parameter values should be url-encoded), `HTTP_BODY`.
```
Request request(Request::Operation::GENERIC_PUBLIC_REQUEST, "okx", "", "Check Server Time");
request.appendParam({
    {"HTTP_METHOD", "GET"},
    {"HTTP_PATH", "/api/v5/public/time"},
});
```

#### Make generic public subscriptions

Instantiate `Subscription` with empty instrument, field `GENERIC_PUBLIC_SUBSCRIPTION` and options set to be the desired websocket payload.
```
Subscription subscription("okx", "", "GENERIC_PUBLIC_SUBSCRIPTION", R"({"type":"subscribe","channels":[{"name":"status"}]})");
```

#### Send generic private requests

Instantiate `Request` with operation `GENERIC_PRIVATE_REQUEST`. Provide request parameters `HTTP_METHOD`, `HTTP_PATH`, and optionally `HTTP_QUERY_STRING` (query string parameter values should be url-encoded), `HTTP_BODY`.
```
Request request(Request::Operation::GENERIC_PRIVATE_REQUEST, "okx", "", "close all positions");
request.appendParam({
    {"HTTP_METHOD", "POST"},
    {"HTTP_PATH", "/api/v5/trade/close-position"},
    {"HTTP_BODY", R"({
      "instId": "BTC-USDT-SWAP",
      "mgnMode": "cross"
  })"},
});
```

### Simple Execution Management

**Objective 1:**

For a specific exchange and instrument, submit a simple limit order.

**Code 1:**

[C++](example/src/execution_management_simple_request/main.cpp) / [Python](binding/python/example/execution_management_simple_request/main.py) / [Java](binding/java/example/execution_management_simple_request/Main.java) / [C#](binding/csharp/example/execution_management_simple_request/MainProgram.cs) / [Go](binding/go/example/execution_management_simple_request/main.go) / [Javascript](binding/javascript/example/execution_management_simple_request/index.js)
```
#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;  // This line is needed.

class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    std::cout << "Received an event:\n" + event.toStringPretty(2, 2) << std::endl;
    return true;
  }
};
} /* namespace ccapi */

using ::ccapi::MyEventHandler;
using ::ccapi::Request;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;
using ::ccapi::toString;
using ::ccapi::UtilSystem;

int main(int argc, char** argv) {
  if (UtilSystem::getEnvAsString("OKX_API_KEY").empty()) {
    std::cerr << "Please set environment variable OKX_API_KEY" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_SECRET").empty()) {
    std::cerr << "Please set environment variable OKX_API_SECRET" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_PASSPHRASE").empty()) {
    std::cerr << "Please set environment variable OKX_API_PASSPHRASE" << std::endl;
    return EXIT_FAILURE;
  }

  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;
  MyEventHandler eventHandler;
  Session session(sessionOptions, sessionConfigs, &eventHandler);
  Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT");
  request.appendParam({
      {"SIDE", "BUY"},
      {"QUANTITY", "0.0005"},
      {"LIMIT_PRICE", "100000"},
  });
  session.sendRequest(request);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  session.stop();
  std::cout << "Bye" << std::endl;
  return EXIT_SUCCESS;
}

```

**Output 1:**
```console
Received an event:
  Event [
    type = RESPONSE,
    messageList = [
      Message [
        type = CREATE_ORDER,
        recapType = UNKNOWN,
        time = 1970-01-01T00:00:00.000000000Z,
        timeReceived = 2021-05-25T03:47:15.599562000Z,
        elementList = [
          Element [
            nameValueMap = {
              CLIENT_ORDER_ID = wBgmzOJbbMTCLJlwTrIeiH,
              CUMULATIVE_FILLED_PRICE_TIMES_QUANTITY = 0,
              CUMULATIVE_FILLED_QUANTITY = 0,
              INSTRUMENT = BTC-USDT,
              LIMIT_PRICE = 100000,
              ORDER_ID = 383781246,
              QUANTITY = 0.0005,
              SIDE = BUY,
              STATUS = live
            }
          ]
        ],
        correlationIdList = [ 5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE ]
      ]
    ]
  ]
Bye
```
* Request operation types: `CREATE_ORDER`, `CANCEL_ORDER`, `GET_ORDER`, `GET_OPEN_ORDERS`, `CANCEL_OPEN_ORDERS`, `GET_ACCOUNTS`, `GET_ACCOUNT_BALANCES`, `GET_ACCOUNT_POSITIONS`.
* Request parameter names: `SIDE`, `QUANTITY`, `LIMIT_PRICE`, `ACCOUNT_ID`, `ACCOUNT_TYPE`, `ORDER_ID`, `CLIENT_ORDER_ID`, `PARTY_ID`, `ORDER_TYPE`, `LEVERAGE`. Instead of these convenient names you can also choose to use arbitrary parameter names and they will be passed to the exchange's native API. See [this example](example/src/execution_management_advanced_request/main.cpp).

**Objective 2:**

For a specific exchange and instrument, receive order updates.

**Code 2:**

[C++](example/src/execution_management_simple_subscription/main.cpp) / [Python](binding/python/example/execution_management_simple_subscription/main.py) / [Java](binding/java/example/execution_management_simple_subscription/Main.java) / [C#](binding/csharp/example/execution_management_simple_subscription/MainProgram.cs) / [Go](binding/go/example/execution_management_simple_subscription/main.go) / [Javascript](binding/javascript/example/execution_management_simple_subscription/index.js)
```
#include "ccapi_cpp/ccapi_session.h"

namespace ccapi {
Logger* Logger::logger = nullptr;  // This line is needed.

class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    if (event.getType() == Event::Type::SUBSCRIPTION_STATUS) {
      std::cout << "Received an event of type SUBSCRIPTION_STATUS:\n" + event.toStringPretty(2, 2) << std::endl;
      auto message = event.getMessageList().at(0);
      if (message.getType() == Message::Type::SUBSCRIPTION_STARTED) {
        Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT");
        request.appendParam({
            {"SIDE", "BUY"},
            {"LIMIT_PRICE", "20000"},
            {"QUANTITY", "0.001"},
            {"CLIENT_ORDER_ID", "6d4eb0fb"},
        });
        session->sendRequest(request);
      }
    } else if (event.getType() == Event::Type::SUBSCRIPTION_DATA) {
      std::cout << "Received an event of type SUBSCRIPTION_DATA:\n" + event.toStringPretty(2, 2) << std::endl;
    }
    return true;
  }
};
} /* namespace ccapi */

using ::ccapi::MyEventHandler;
using ::ccapi::Request;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;
using ::ccapi::Subscription;
using ::ccapi::UtilSystem;

int main(int argc, char** argv) {
  if (UtilSystem::getEnvAsString("OKX_API_KEY").empty()) {
    std::cerr << "Please set environment variable OKX_API_KEY" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_SECRET").empty()) {
    std::cerr << "Please set environment variable OKX_API_SECRET" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_PASSPHRASE").empty()) {
    std::cerr << "Please set environment variable OKX_API_PASSPHRASE" << std::endl;
    return EXIT_FAILURE;
  }
  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;
  MyEventHandler eventHandler;
  Session session(sessionOptions, sessionConfigs, &eventHandler);
  Subscription subscription("okx", "BTC-USDT", "ORDER_UPDATE");
  session.subscribe(subscription);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  session.stop();
  std::cout << "Bye" << std::endl;
  return EXIT_SUCCESS;
}

```

**Output 2:**
```console
Received an event of type SUBSCRIPTION_STATUS:
  Event [
    type = SUBSCRIPTION_STATUS,
    messageList = [
      Message [
        type = SUBSCRIPTION_STARTED,
        recapType = UNKNOWN,
        time = 1970-01-01T00:00:00.000000000Z,
        timeReceived = 2021-05-25T04:22:25.906197000Z,
        elementList = [

        ],
        correlationIdList = [ 5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE ]
      ]
    ]
  ]
Received an event of type SUBSCRIPTION_DATA:
  Event [
    type = SUBSCRIPTION_DATA,
    messageList = [
      Message [
        type = EXECUTION_MANAGEMENT_EVENTS_ORDER_UPDATE,
        recapType = UNKNOWN,
        time = 2021-05-25T04:22:26.653785000Z,
        timeReceived = 2021-05-25T04:22:26.407419000Z,
        elementList = [
          Element [
            nameValueMap = {
              CLIENT_ORDER_ID = ,
              INSTRUMENT = BTC-USDT,
              LIMIT_PRICE = 20000,
              ORDER_ID = 6ca39186-be79-4777-97ab-1695fccd0ce4,
              QUANTITY = 0.001,
              SIDE = BUY,
              STATUS = live
            }
          ]
        ],
        correlationIdList = [ 5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE ]
      ]
    ]
  ]
Bye
```
* Subscription fields: `ORDER_UPDATE`, `PRIVATE_TRADE`, `BALANCE_UPDATE`, `POSITION_UPDATE`.

### Advanced Execution Management

#### Specify correlation id

Instantiate `Request` with the desired correlationId. The `correlationId` should be unique.
```
Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT", "cool correlation id");
```
Instantiate `Subscription` with the desired correlationId.
```
Subscription subscription("okx", "BTC-USDT", "ORDER_UPDATE", "", "cool correlation id");
```
This is used to match a particular request or subscription with its returned data. Within each `Message` there is a `correlationIdList` to identify the request or subscription that requested the data.

#### Multiple exchanges and/or instruments

Send a `std::vector<Request>`.
```
Request request_1(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT", "cool correlation id for BTC");
request_1.appendParam(...);
Request request_2(Request::Operation::CREATE_ORDER, "okx", "ETH-USDT", "cool correlation id for ETH");
request_2.appendParam(...);
session.sendRequest({request_1, request_2});
```
Subscribe one `Subscription` per exchange with a comma separated string of instruments.
```
Subscription subscription("okx", "BTC-USDT,ETH-USDT", "ORDER_UPDATE");
```

#### Multiple subscription fields

Subscribe one `Subscription` with a comma separated string of fields.
```
Subscription subscription("okx", "BTC-USDT", "ORDER_UPDATE,PRIVATE_TRADE");
```

#### Make Session::sendRequest blocking
Instantiate `Session` without `EventHandler` argument, and pass a pointer to `Queue<Event>` as an additional argument.
```
Session session(sessionOptions, sessionConfigs);
...
Queue<Event> eventQueue;
session.sendRequest(request, &eventQueue);  // block until a response is received
std::vector<Event> eventList = eventQueue.purge();
```

#### Provide API credentials for an exchange
There are 3 ways to provide API credentials (listed with increasing priority).
* Set the relevent environment variables. Some exchanges might need additional credentials other than API keys and secrets: e.g. `OKX_API_PASSPHRASE`, `KUCOIN_API_PASSPHRASE`. See section "exchange API credentials" in [`include/ccapi_cpp/ccapi_macro.h`](include/ccapi_cpp/ccapi_macro.h).
* Provide credentials to `SessionConfigs`.
```
sessionConfigs.setCredential({
  {"OKX_API_KEY", ...},
  {"OKX_API_SECRET", ...}
});
```
* Provide credentials to `Request` or `Subscription`.
```
Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT", "", {
  {"OKX_API_KEY", ...},
  {"OKX_API_SECRET", ...}
});
```
```
Subscription subscription("okx", "BTC-USDT", "ORDER_UPDATE", "", "", {
  {"OKX_API_KEY", ...},
  {"OKX_API_SECRET", ...}
});
```

#### Override exchange urls
You can override exchange urls at compile time by using macros. See section "exchange REST urls", "exchange WS urls", and "exchange FIX urls" in [`include/ccapi_cpp/ccapi_macro.h`](include/ccapi_cpp/ccapi_macro.h). You can also override exchange urls at runtime. See [this example](example/src/override_exchange_url_at_runtime/main.cpp). These can be useful if you need to connect to test accounts (e.g. https://www.okx.com/docs-v5/en/#overview-demo-trading-services).

#### Complex request parameters
Please follow the exchange's API documentations: e.g. https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order.
```
Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDT");
request.appendParam({
    {"tdMode", "cross"},
    {"ccy", "USDT"},
});
```

#### Send request by Websocket API
```
Subscription subscription("okx", "BTC-USDTT", "ORDER_UPDATE", "", "same correlation id for subscription and request");
session.subscribe(subscription);
...
Request request(Request::Operation::CREATE_ORDER, "okx", "BTC-USDTT", "same correlation id for subscription and request");
request.appendParam({
    {"SIDE", "BUY"},
    {"LIMIT_PRICE", "20000"},
    {"QUANTITY", "0.001"},
});
session.sendRequestByWebsocket(request);
```

#### Specify instrument type
Some exchanges (i.e. bybit) might need instrument type for `Subscription`. Use `Subscription`'s `setInstrumentType` method.
```
Subscription subscription("bybit", "BTCUSDTT", "MARKET_DEPTH");
subscription.setInstrumentType("spot");
session.subscribe(subscription);
```

### FIX API

**Objective:**

For a specific exchange and instrument, submit a simple limit order.

**Code:**

[C++](example/src/fix_simple/main.cpp) / [Python](binding/python/example/fix_simple/main.py) / [Java](binding/java/example/fix_simple/Main.java) / [C#](binding/csharp/example/fix_simple/MainProgram.cs) / [Go](binding/go/example/fix_simple/main.go) / [Javascript](binding/javascript/example/fix_simple/index.js)
```
#include "ccapi_cpp/ccapi_session.h"
namespace ccapi {
Logger* Logger::logger = nullptr;  // This line is needed.
class MyEventHandler : public EventHandler {
 public:
  bool processEvent(const Event& event, Session* session) override {
    if (event.getType() == Event::Type::AUTHORIZATION_STATUS) {
      std::cout << "Received an event of type AUTHORIZATION_STATUS:\n" + event.toStringPretty(2, 2) << std::endl;
      auto message = event.getMessageList().at(0);
      if (message.getType() == Message::Type::AUTHORIZATION_SUCCESS) {
        Request request(Request::Operation::FIX, "okx", "", "same correlation id for subscription and request");
        request.appendParamFix({
            {35, "D"},
            {11, "6d4eb0fb-2229-469f-873e-557dd78ac11e"},
            {55, "BTC-USDT"},
            {54, "1"},
            {44, "20000"},
            {38, "0.001"},
            {40, "2"},
            {59, "1"},
        });
        session->sendRequestByFix(request);
      }
    } else if (event.getType() == Event::Type::FIX) {
      std::cout << "Received an event of type FIX:\n" + event.toStringPretty(2, 2) << std::endl;
    }
    return true;
  }
};
} /* namespace ccapi */
using ::ccapi::MyEventHandler;
using ::ccapi::Session;
using ::ccapi::SessionConfigs;
using ::ccapi::SessionOptions;
using ::ccapi::Subscription;
using ::ccapi::UtilSystem;
int main(int argc, char** argv) {
  if (UtilSystem::getEnvAsString("OKX_API_KEY").empty()) {
    std::cerr << "Please set environment variable OKX_API_KEY" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_SECRET").empty()) {
    std::cerr << "Please set environment variable OKX_API_SECRET" << std::endl;
    return EXIT_FAILURE;
  }
  if (UtilSystem::getEnvAsString("OKX_API_PASSPHRASE").empty()) {
    std::cerr << "Please set environment variable OKX_API_PASSPHRASE" << std::endl;
    return EXIT_FAILURE;
  }
  SessionOptions sessionOptions;
  SessionConfigs sessionConfigs;
  MyEventHandler eventHandler;
  Session session(sessionOptions, sessionConfigs, &eventHandler);
  Subscription subscription("okx", "", "FIX", "", "same correlation id for subscription and request");
  session.subscribeByFix(subscription);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  session.stop();
  std::cout << "Bye" << std::endl;
  return EXIT_SUCCESS;
}
```
**Output:**
```console
Received an event of type AUTHORIZATION_STATUS:
  Event [
    type = AUTHORIZATION_STATUS,
    messageList = [
      Message [
        type = AUTHORIZATION_SUCCESS,
        recapType = UNKNOWN,
        time = 1970-01-01T00:00:00.000000000Z,
        timeReceived = 2021-05-25T05:05:15.892366000Z,
        elementList = [
          Element [
            tagValueMap = {
              96 = 0srtt0WetUTYHiTpvyWnC+XKKHCzQQIJ/8G9lE4KVxM=,
              98 = 0,
              108 = 15,
              554 = 26abh7of52i
            }
          ]
        ],
        correlationIdList = [ same correlation id for subscription and request ]
      ]
    ]
  ]
Received an event of type FIX:
  Event [
    type = FIX,
    messageList = [
      Message [
        type = FIX,
        recapType = UNKNOWN,
        time = 1970-01-01T00:00:00.000000000Z,
        timeReceived = 2021-05-25T05:05:15.984090000Z,
        elementList = [
          Element [
            tagValueMap = {
              11 = 6d4eb0fb-2229-469f-873e-557dd78ac11e,
              17 = b7caec79-1bc8-460e-af28-6489cf12f45e,
              20 = 0,
              37 = 458acfe5-bdea-46d2-aa87-933cda84163f,
              38 = 0.001,
              39 = 0,
              44 = 20000,
              54 = 1,
              55 = BTC-USDT,
              60 = 20210525-05:05:16.008,
              150 = 0
            }
          ]
        ],
        correlationIdList = [ same correlation id for subscription and request ]
      ]
    ]
  ]
Bye
```

### More Advanced Topics

#### Handle events in "immediate" vs. "batching" mode

In general there are 2 ways to handle events.
* When a `Session` is instantiated with an `eventHandler` argument, it will handle events in immediate mode. The `processEvent` method in the `eventHandler` will be invoked immediately when an `Event` is available.
* When a `Session` is instantiated without an `eventHandler` argument, it will handle events in batching mode. The evetns will be batched into an internal `Queue<Event>` and can be retrieved by
```
std::vector<Event> eventList = session.getEventQueue().purge();
```
An example can be found [here](example/src/market_data_advanced_subscription/main.cpp).

#### Thread safety
* The following methods are implemented to be thread-safe: `Session::sendRequest`, `Session::subscribe`, `Session::sendRequestByFix`, `Session::subscribeByFix`, `Session::setTimer`, all public methods in `Queue`.
* The `processEvent` method in the `eventHandler` is invoked on one of the internal threads in the `eventDispatcher`. A default `EventDispatcher` with 1 internal thread will be created if no `eventDispatcher` argument is provided in `Session` instantiation. To dispatch events to multiple threads, instantiate `EventDispatcher` with `numDispatcherThreads` set to be the desired number. `EventHandler`s and/or `EventDispatcher`s can be shared among different sessions. Otherwise, different sessions are independent from each other.
```
EventDispatcher eventDispatcher(2);
Session session(sessionOptions, sessionConfigs, &eventHandler, &eventDispatcher);
```
An example can be found [here](example/src/market_data_advanced_subscription/main.cpp).

#### Enable library logging

[C++](example/src/enable_library_logging/main.cpp) / [Python](binding/python/example/enable_library_logging/main.py) / [Java](binding/java/example/enable_library_logging/Main.java) / [C#](binding/csharp/example/enable_library_logging/MainProgram.cs) / [Go](binding/go/example/enable_library_logging/main.go)

Extend a subclass, e.g. `MyLogger`, from class `Logger` and override method `logMessage`. Assign a `MyLogger` pointer to `Logger::logger`. Add one of the following macros in the compiler command line: `CCAPI_ENABLE_LOG_TRACE`, `CCAPI_ENABLE_LOG_DEBUG`, `CCAPI_ENABLE_LOG_INFO`, `CCAPI_ENABLE_LOG_WARN`, `CCAPI_ENABLE_LOG_ERROR`, `CCAPI_ENABLE_LOG_FATAL`. Enable logging if you'd like to inspect raw responses/messages from the exchange for troubleshooting purposes.
```
namespace ccapi {
class MyLogger final : public Logger {
 public:
  void logMessage(const std::string& severity, const std::string& threadId, const std::string& timeISO, const std::string& fileName,
                  const std::string& lineNumber, const std::string& message) override {
    std::lock_guard<std::mutex> lock(m);
    std::cout << threadId << ": [" << timeISO << "] {" << fileName << ":" << lineNumber << "} " << severity << std::string(8, ' ') << message << std::endl;
  }

 private:
  std::mutex m;
};

MyLogger myLogger;
Logger* Logger::logger = &myLogger;
} /* namespace ccapi */
```

#### Set timer

[C++](example/src/utility_set_timer/main.cpp)

To perform an asynchronous wait, use the utility method `setTimer` in class `Session`. The handlers are invoked in the same threads as the `processEvent` method in the `EventHandler` class. The `id` of the timer should be unique. `delayMilliseconds` can be 0.
```
session->setTimer(
    "id", 1000,
    [](const boost::system::error_code&) {
      std::cout << std::string("Timer error handler is triggered at ") + UtilTime::getISOTimestamp(UtilTime::now()) << std::endl;
    },
    []() { std::cout << std::string("Timer success handler is triggered at ") + UtilTime::getISOTimestamp(UtilTime::now()) << std::endl; });
```

## Performance Tuning
* Turn on compiler optimization flags (e.g. `cmake -DCMAKE_BUILD_TYPE=Release ...`).
* Enable link time optimization (e.g. in CMakeLists.txt `set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)` before a target is created). Note that link time optimization is only applicable to static linking.
* Shorten constant strings used as key names in the returned `Element` (e.g. in CmakeLists.txt `add_compile_definitions(CCAPI_BEST_BID_N_PRICE="b")`).
* Only enable the services and exchanges that you need.
* Use FIX API instead of REST API.
* Handle events in ["batching" mode](#handle-events-in-immediate-vs-batching-mode) if your application (e.g. market data archiver) isn't latency sensitive.
* Define macro `CCAPI_USE_SINGLE_THREAD`. It reduces locking overhead for single threaded applications.

## Known Issues and Workarounds
* Kraken invalid nonce errors. Give the API key a nonce window (https://support.kraken.com/hc/en-us/articles/360001148023-What-is-a-nonce-window-). We use unix timestamp with microsecond resolution as nonce and therefore a nonce window of 500000 translates to a tolerance of 0.5 second.

## Contributing
* (Required) Create a new branch from the `develop` branch and submit a pull request to the `develop` branch.
* (Optional) C++ code style: https://google.github.io/styleguide/cppguide.html. See file [.clang-format](.clang-format).
* (Optional) Commit message format: https://conventionalcommits.org.
