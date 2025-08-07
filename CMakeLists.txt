# CMake minimum version requirement
cmake_minimum_required(VERSION 3.20)

# Define the project
project(KrakenL3Collector LANGUAGES CXX)

cmake_policy(SET CMP0167 NEW)

if (MSVC)
  # For Debug builds, switch MSVC runtime to the Debug DLL (/MDd)
  string(REPLACE "/MD" "/MDd" CMAKE_C_FLAGS_DEBUG     "${CMAKE_C_FLAGS_DEBUG}")
  string(REPLACE "/MD" "/MDd" CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG}")
endif()

# Set the default build type to be an optimized release with debug info
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE) #DEBUG
endif()

# Set the C++ standard to 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- Find Dependencies managed by vcpkg ---
# We still use vcpkg for our core dependencies.
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(ZLIB REQUIRED)

# --- Fetch QuestDB C++ Client directly from GitHub ---
# This section replaces find_package(questdb-cpp).
include(FetchContent)
FetchContent_Declare(
  questdb_cpp_client
  GIT_REPOSITORY https://github.com/questdb/c-questdb-client.git
  GIT_TAG        4.0.5  # Use a specific, stable release tag instead of "main"
)
set(QUESTDB_SYSTEM_TESTING OFF CACHE BOOL "Disable QuestDB system tests" FORCE)

# This command will download and configure the QuestDB client.
# It will also automatically fetch its own dependency (the C client).
FetchContent_MakeAvailable(questdb_cpp_client)

if (WIN32)
  # target Windows 7+ APIs; change to 0x0A00 for Win10, etc.
  add_compile_definitions(_WIN32_WINNT=0x0A00)
endif()

# --- Define Your Executable ---
add_executable(
    kraken_l3_collector
    example/src/kraken_l3_collector.cpp
)
if (MSVC)
  # Allow MSVC to generate large object files
  target_compile_options(kraken_l3_collector
    PRIVATE
      /bigobj
  )
endif()

# --- Link Dependencies to Your Executable ---
target_link_libraries(
    kraken_l3_collector
    PRIVATE
    questdb_client          # ← this is the Rust/C library target imported by Corrosion

    # …your other libraries…
    Boost::boost
    OpenSSL::SSL
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
    ZLIB::ZLIB
)
if (WIN32)
  target_link_libraries(
    kraken_l3_collector
    PRIVATE
      # Windows system libs
      Ws2_32
      User32
      Crypt32
      Bcrypt
  )
endif()

# --- Add Include Directories ---
target_include_directories(
    kraken_l3_collector
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# --- Set Compiler Definitions ---
target_compile_definitions(
    kraken_l3_collector
    PRIVATE
    CCAPI_ENABLE_SERVICE_MARKET_DATA
    # CCAPI_ENABLE_LOG_TRACE
    CCAPI_ENABLE_SERVICE_EXECUTION_MANAGEMENT
    CCAPI_ENABLE_EXCHANGE_KRAKEN
    _CRT_SECURE_NO_WARNINGS
)