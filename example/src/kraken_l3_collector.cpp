// kraken_hourly_collector_L3.cpp

// MODIFICATION: Include the specific service headers FIRST.
#include "ccapi_cpp/service/ccapi_execution_management_service_kraken.h"
#include "ccapi_cpp/service/ccapi_market_data_service_kraken.h"
#include "json_utils.h"  // ← for safeGetString()

// Now, include the rest of the CCAPI headers
#include <zlib.h>  // For zlib compression

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>  // for std::getenv
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <questdb/ingress/line_sender.hpp>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <variant>
#include <vector>

#include "ccapi_cpp/ccapi_element.h"
#include "ccapi_cpp/ccapi_event.h"
#include "ccapi_cpp/ccapi_event_dispatcher.h"
#include "ccapi_cpp/ccapi_event_handler.h"
#include "ccapi_cpp/ccapi_logger.h"  // For Logger base class
#include "ccapi_cpp/ccapi_macro.h"
#include "ccapi_cpp/ccapi_message.h"
#include "ccapi_cpp/ccapi_request.h"
#include "ccapi_cpp/ccapi_session.h"
#include "ccapi_cpp/ccapi_subscription.h"
#include "ccapi_cpp/ccapi_util.h"

// Using directives for QuestDB convenience
using namespace questdb::ingress::literals;

// Global CCAPI Logger Setup
class MyGlobalCcapiLogger : public ccapi::Logger {
 public:
  void logMessage(const std::string& severity, const std::string& threadId, const std::string& timeISO, const std::string& fileName,
                  const std::string& lineNumber, const std::string& message) override {
    std::cout << "[CCAPI Internal Log| " << severity << " | " << threadId << " | "
              << /*fileName << ":" << lineNumber <<*/ " ] "  // Commented out file/line for brevity
              << message << std::endl;
  }
};

std::string doubleToStringWithMaxPrecision(double value) {
  std::ostringstream out;
  out << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
  return out.str();
}

void trim(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
}

MyGlobalCcapiLogger globalCcapiLoggerInstance;

namespace ccapi {
Logger* Logger::logger = &globalCcapiLoggerInstance;
}  // namespace ccapi

namespace kraken_l3_collector_questdb {

const int PRICE_PRECISION = 7;              // Use 7 decimal places of precision
const int64_t PRICE_MULTIPLIER = 10000000;  // 10^7

const int SIZE_PRECISION = 8;
const int64_t SIZE_MULTIPLIER = 100000000;  // 10^8

inline int64_t priceToLong(double price) { return std::llround(price * PRICE_MULTIPLIER); }

inline double longToPrice(int64_t price_long) { return static_cast<double>(price_long) / PRICE_MULTIPLIER; }

inline int64_t sizeToLong(double size) { return static_cast<int64_t>(std::round(size * SIZE_MULTIPLIER)); }

inline double longToSize(int64_t size_long) { return static_cast<double>(size_long) / SIZE_MULTIPLIER; }

const std::string QUESTDB_HOST = "localhost";
const int QUESTDB_ILP_TCP_PORT = 9009;
const int NUM_AGGREGATED_LEVELS_TO_SEND = 8;
const int CRC_DEPTH = 10;
const size_t MAX_BOOK_DEPTH = 10;
const size_t PRUNE_HIGH_WATER_MARK = 10;

int64_t decimalStrToLong(const std::string& s, int precision, int64_t multiplier) {
  if (s.empty()) {
    return 0;
  }

  auto dot_pos = s.find('.');
  std::string int_part_str;
  std::string frac_part_str;

  if (dot_pos == std::string::npos) {
    int_part_str = s;
    frac_part_str = "";
  } else {
    int_part_str = s.substr(0, dot_pos);
    frac_part_str = s.substr(dot_pos + 1);
  }

  // Handle empty integer part, e.g., ".5"
  if (int_part_str == "" || int_part_str == "-") {
    int_part_str.push_back('0');
  }

  // Normalize fractional part to the required precision
  if (frac_part_str.length() > precision) {
    frac_part_str.resize(precision);
  } else {
    frac_part_str.append(precision - frac_part_str.length(), '0');
  }

  try {
    int64_t int_part = std::stoll(int_part_str);
    int64_t frac_part = frac_part_str.empty() ? 0 : std::stoll(frac_part_str);

    if (int_part < 0 || (s.length() > 0 && s[0] == '-')) {
      return int_part * multiplier - frac_part;
    } else {
      return int_part * multiplier + frac_part;
    }
  } catch (const std::exception& e) {
    // Log or handle the error if the string is not a valid number
    return 0;
  }
}

inline int64_t priceStrToLong(const std::string& s) { return decimalStrToLong(s, PRICE_PRECISION, PRICE_MULTIPLIER); }

inline int64_t sizeStrToLong(const std::string& s) { return decimalStrToLong(s, SIZE_PRECISION, SIZE_MULTIPLIER); }

struct L3Order {
  std::string orderId;
  int64_t size_l{0};
  ccapi::TimePoint arrivalTime;

  std::string priceStr;
  std::string sizeStr;

  L3Order(std::string id = "", int64_t sz_l = 0, ccapi::TimePoint tp = {}, std::string price_s = "", std::string size_s = "")
      : orderId(std::move(id)), size_l(sz_l), arrivalTime(tp), priceStr(std::move(price_s)), sizeStr(std::move(size_s)) {}
};

struct L3PriceLevel {
  int64_t price_l;
  std::string price;  // The original, clean price string
  std::list<L3Order> orders;
  std::map<std::string, std::list<L3Order>::iterator> orderId_to_iter;
  int64_t totalSizeAtLevel_l{0};

  L3PriceLevel(int64_t p_l = 0, std::string p_s = "") : price_l(p_l), price(std::move(p_s)) {}

  // This is the definitive fix for the memory corruption issue. It prevents
  // the std::map from creating dangling iterators during reallocations.
  L3PriceLevel(const L3PriceLevel&) = delete;
  L3PriceLevel& operator=(const L3PriceLevel&) = delete;
  L3PriceLevel(L3PriceLevel&&) = delete;
  L3PriceLevel& operator=(L3PriceLevel&&) = delete;

  void applyChange(const std::string& orderId, int64_t newSize_l, const ccapi::TimePoint& messageTime, const std::string& priceStr,
                   const std::string& sizeStr) {
    auto it = orderId_to_iter.find(orderId);
    if (it != orderId_to_iter.end()) {
      auto list_iter = it->second;
      totalSizeAtLevel_l -= list_iter->size_l;

      if (newSize_l <= 0) {
        orders.erase(list_iter);
        orderId_to_iter.erase(it);
      } else {
        list_iter->size_l = newSize_l;
        if (!priceStr.empty()) list_iter->priceStr = priceStr;
        if (!sizeStr.empty()) list_iter->sizeStr = sizeStr;
        totalSizeAtLevel_l += newSize_l;
      }
    } else {
      if (newSize_l > 0) {
        orders.emplace_back(orderId, newSize_l, messageTime, priceStr, sizeStr);
        auto new_iter = std::prev(orders.end());
        orderId_to_iter[orderId] = new_iter;
        totalSizeAtLevel_l += newSize_l;
      }
    }
  }

  // Helper functions remain the same...
  std::string getTotalSizeAtLevelStr() const {
    std::ostringstream out;
    out << std::fixed << std::setprecision(SIZE_PRECISION) << longToSize(totalSizeAtLevel_l);
    std::string s = out.str();
    // Trim trailing zeros and the decimal point if it's left at the end
    s.erase(s.find_last_not_of('0') + 1, std::string::npos);
    if (s.back() == '.') {
      s.pop_back();
    }
    return s;
  }

  size_t getNumOrdersAtLevel() const { return orders.size(); }
};

struct AggregatedLevelData {
  int64_t price_l;
  int64_t size_l;
  int64_t orders_count;

  void clear() {
    price_l = 0;
    size_l = 0;
    orders_count = 0;
  }

  // Add operator== for easy comparison of std::array
  bool operator==(const AggregatedLevelData& other) const { return price_l == other.price_l && size_l == other.size_l && orders_count == other.orders_count; }
};

struct CalculatedLevelFeatures {
  int64_t orders80pct = 0;
  double hhi = 0.0;
  double topOrderSize = 0.0;
  int64_t topOrderAge_ms = 0;
};

struct CalculatedFlowFeatures {
  std::string assetPair;
  double ofi_level1 = 0.0, ofi_level2 = 0.0, ofi_level3 = 0.0, ofi_level4 = 0.0, ofi_level5 = 0.0;
  ccapi::TimePoint tp;
};

struct L3DataForQueue {
  std::string exchange;
  std::string assetPair;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topBids;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topAsks;
  std::array<CalculatedLevelFeatures, 5> topBidsFeatures;
  std::array<CalculatedLevelFeatures, 5> topAsksFeatures;
  ccapi::TimePoint tp;
};

struct TradeDataForQueue {
  std::string exchange;
  std::string assetPair;
  double price_d;
  double size_d;
  std::string side;
  std::string ord_type;
  ccapi::TimePoint tp;
};

struct SimpleOrder {
  int64_t size_l;
  ccapi::TimePoint arrivalTime;
};

struct SimplePriceLevel {
  int64_t price_l;
  int64_t totalSizeAtLevel_l;
  std::vector<SimpleOrder> orders;
};

struct BookDataForProcessing {
  ccapi::TimePoint tp;
  std::string assetPair;
  std::string exchange;
  bool is_snapshot{false};
  bool top_5_levels_changed{false};
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topBids;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topAsks;
  std::vector<SimplePriceLevel> top_5_bids_levels;
  std::vector<SimplePriceLevel> top_5_asks_levels;
  std::map<int64_t, int64_t> prev_bid_volumes_l;
  std::map<int64_t, int64_t> prev_ask_volumes_l;
};

using QdbEvent = std::variant<L3DataForQueue, TradeDataForQueue, CalculatedFlowFeatures>;

template <typename T>
class ThreadSafeQueue {
 public:
  ThreadSafeQueue() : shutdown_(false) {}

  void push(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) return;
    queue_.push(std::move(value));
    cond_var_.notify_one();
  }

  size_t size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

  bool wait_and_pop(T& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_var_.wait(lock, [this] { return !queue_.empty() || shutdown_; });
    if (shutdown_ && queue_.empty()) {
      return false;
    }
    value = std::move(queue_.front());
    queue_.pop();
    return true;
  }

  void shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
    cond_var_.notify_all();
  }

  bool try_pop(T& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty() || shutdown_) {
      return false;
    }
    value = std::move(queue_.front());
    queue_.pop();
    return true;
  }

 private:
  mutable std::mutex mutex_;
  std::queue<T> queue_;
  std::condition_variable cond_var_;
  bool shutdown_;
};

struct InstrumentState {
  std::map<std::string, std::pair<int64_t, std::string>> orderIdToSideAndPriceKey;
  std::unordered_set<std::string> pendingPriceChangeDeletes_;
  std::string exchange;
  std::string assetPair;
  std::map<int64_t, L3PriceLevel, std::greater<int64_t>> bidBookL3;
  std::map<int64_t, L3PriceLevel> askBookL3;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topBidsForQuest;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topAsksForQuest;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> lastSentTopBids;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> lastSentTopAsks;
  std::array<AggregatedLevelData, 5> lastSentTop5Bids;
  std::array<AggregatedLevelData, 5> lastSentTop5Asks;
  bool bookSnapshotReceived{false};
  std::map<int64_t, int64_t> prevBidVolumes_l;
  std::map<int64_t, int64_t> prevAskVolumes_l;
  std::vector<ccapi::Message> pendingUpdates;
  std::map<std::string, std::chrono::steady_clock::time_point> recentlyProcessedDeletes_;
  const std::chrono::seconds RECENTLY_PROCESSED_DELETE_TTL = std::chrono::seconds(10);
  const size_t RECENTLY_PROCESSED_DELETE_MAX_SIZE = 4000;
  ccapi::TimePoint last_trade_timestamp;
  std::string previousLogBlock;
  std::string currentLogBlock;
  bool checksumMismatchDeferred{false};  // Initialize to false
  std::chrono::steady_clock::time_point lastChecksumVerificationTime;
  int deferChecksumCounter{0};
  std::atomic<bool> resyncInProgress{false};

  void dumpBookStateToLog(std::stringstream& log_buffer) const {
    log_buffer << "[DEBUG] --- BEGIN BOOK STATE DUMP ---" << std::endl;
    auto printTopOrders = [&](const auto& book, const std::string& side) {
      int level_count = 0;
      for (const auto& pair : book) {
        if (++level_count > 10) break;
        log_buffer << "[DEBUG] " << side << " L" << level_count << " Price: " << pair.second.price << std::endl;
        for (const auto& order : pair.second.orders) {
          log_buffer << "[DEBUG]   -> Order ID: " << order.orderId << ", Size: " << order.sizeStr
                     << ", Time: " << ccapi::UtilTime::getISOTimestamp(order.arrivalTime) << std::endl;
        }
      }
    };
    printTopOrders(this->askBookL3, "ASKS");
    printTopOrders(this->bidBookL3, "BIDS");
    log_buffer << "[DEBUG] --- END BOOK STATE DUMP ---" << std::endl;
  }

  void pruneDeleteCache() {
    // Prune if the size is over the high water mark OR just periodically to keep it small
    if (recentlyProcessedDeletes_.empty()) {
      return;
    }

    auto now = std::chrono::steady_clock::now();
    for (auto it = recentlyProcessedDeletes_.begin(); it != recentlyProcessedDeletes_.end();) {
      if (std::chrono::duration_cast<std::chrono::seconds>(now - it->second) > RECENTLY_PROCESSED_DELETE_TTL) {
        it = recentlyProcessedDeletes_.erase(it);
      } else {
        ++it;
      }
    }
  }

  void clearL3Book() {
    bidBookL3.clear();
    askBookL3.clear();
    orderIdToSideAndPriceKey.clear();

    for (auto& l : topBidsForQuest) l.clear();
    for (auto& l : topAsksForQuest) l.clear();
    for (auto& l : lastSentTopBids) l.clear();
    for (auto& l : lastSentTopAsks) l.clear();

    pendingUpdates.clear();

    recentlyProcessedDeletes_.clear();
    bookSnapshotReceived = false;
    checksumMismatchDeferred = false;
    deferChecksumCounter = 0;
    lastChecksumVerificationTime = std::chrono::steady_clock::time_point{};  // Reset the time
  }

  void pruneBeyondDepth(size_t max_depth) {
    // Prune the bid side if it exceeds the max depth.
    while (bidBookL3.size() > max_depth) {
      auto worstBidIt = std::prev(bidBookL3.end());
      for (const auto& order : worstBidIt->second.orders) {
        orderIdToSideAndPriceKey.erase(order.orderId);
      }
      bidBookL3.erase(worstBidIt);
    }

    // Prune the ask side if it exceeds the max depth.
    while (askBookL3.size() > max_depth) {
      auto worstAskIt = std::prev(askBookL3.end());
      for (const auto& order : worstAskIt->second.orders) {
        orderIdToSideAndPriceKey.erase(order.orderId);
      }
      askBookL3.erase(worstAskIt);
    }
  }

  void updateTopNForQuestDB() {
    for (auto& l : topBidsForQuest) l.clear();
    for (auto& l : topAsksForQuest) l.clear();
    int i = 0;
    for (const auto& pair_ : bidBookL3) {
      if (i >= NUM_AGGREGATED_LEVELS_TO_SEND) break;
      topBidsForQuest[i].price_l = pair_.second.price_l;
      topBidsForQuest[i].size_l = pair_.second.totalSizeAtLevel_l;
      topBidsForQuest[i].orders_count = pair_.second.getNumOrdersAtLevel();
      i++;
    }
    i = 0;
    for (const auto& pair_ : askBookL3) {
      if (i >= NUM_AGGREGATED_LEVELS_TO_SEND) break;
      topAsksForQuest[i].price_l = pair_.second.price_l;
      topAsksForQuest[i].size_l = pair_.second.totalSizeAtLevel_l;
      topAsksForQuest[i].orders_count = pair_.second.getNumOrdersAtLevel();
      i++;
    }
  }

  static CalculatedLevelFeatures calculate_level_features_from_simple(const SimplePriceLevel& level, const ccapi::TimePoint& currentTime) {
    CalculatedLevelFeatures features;
    if (level.orders.empty()) {
      return features;
    }

    // Find the order with the earliest arrival time (top of queue)
    const SimpleOrder* topOrder = &level.orders[0];
    for (size_t i = 1; i < level.orders.size(); ++i) {
      if (level.orders[i].arrivalTime < topOrder->arrivalTime) {
        topOrder = &level.orders[i];
      }
    }

    features.topOrderSize = longToSize(topOrder->size_l);
    features.topOrderAge_ms = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - topOrder->arrivalTime).count();

    std::vector<double> orderSizes;
    orderSizes.reserve(level.orders.size());
    for (const auto& order : level.orders) {
      orderSizes.push_back(longToSize(order.size_l));
    }

    double totalVolume = longToSize(level.totalSizeAtLevel_l);
    if (totalVolume > 1e-9) {
      std::sort(orderSizes.rbegin(), orderSizes.rend());
      double volumeSum80 = 0.0;
      int ordersCount80 = 0;
      for (double size : orderSizes) {
        volumeSum80 += size;
        ordersCount80++;
        if (volumeSum80 >= totalVolume * 0.8) {
          break;
        }
      }
      features.orders80pct = ordersCount80;

      double hhiSum = 0.0;
      for (double size : orderSizes) {
        double share = size / totalVolume;
        hhiSum += (share * share);
      }
      features.hhi = hhiSum;
    }
    return features;
  }

  static std::array<double, 5> calculate_ofi_from_simple(const std::vector<SimplePriceLevel>& currentBook, const std::map<int64_t, int64_t>& prevVolumes_l) {
    std::array<double, 5> ofi_levels{};
    for (size_t i = 0; i < 5 && i < currentBook.size(); ++i) {
      const auto& level = currentBook[i];
      int64_t price_l = level.price_l;
      double current_volume = longToSize(level.totalSizeAtLevel_l);
      double prev_volume = 0.0;

      auto prev_it = prevVolumes_l.find(price_l);
      if (prev_it != prevVolumes_l.end()) {
        prev_volume = longToSize(prev_it->second);  // Convert here, outside the lock
      }
      ofi_levels[i] = current_volume - prev_volume;
    }
    return ofi_levels;
  }

  void applyQueueResetModify(std::stringstream& log_buffer, const std::string& orderId, const std::string& priceStr, const std::string& sizeStr,
                             const std::string& sideStr, const ccapi::TimePoint& messageTime) {
    auto it = orderIdToSideAndPriceKey.find(orderId);
    if (it == orderIdToSideAndPriceKey.end()) {
      // This can happen if the original 'delete' was for an order we had already pruned.
      // In this case, we just process the 'add' part of the operation.
      log_buffer << "[RECOVERY] Coalesced ADD for untracked order " << orderId << ". Processing as a new 'add'." << std::endl;
      applyL3Update(log_buffer, "add", orderId, priceStr, sizeStr, sideStr, messageTime);
      return;
    }

    int64_t oldPrice_l = it->second.first;
    const std::string& oldSideStr = it->second.second;
    int64_t newSize_l = sizeStrToLong(sizeStr);
    int64_t newPrice_l = priceStrToLong(priceStr);

    // Step A: Remove the order from its old location.
    if (oldSideStr == "bid") {
      auto lvlIt = bidBookL3.find(oldPrice_l);
      if (lvlIt != bidBookL3.end()) {
        lvlIt->second.applyChange(orderId, 0, {}, "", "");
        if (lvlIt->second.orders.empty()) bidBookL3.erase(lvlIt);
      }
    } else {  // ask
      auto lvlIt = askBookL3.find(oldPrice_l);
      if (lvlIt != askBookL3.end()) {
        lvlIt->second.applyChange(orderId, 0, {}, "", "");
        if (lvlIt->second.orders.empty()) askBookL3.erase(lvlIt);
      }
    }

    // Step B: Add the order to its new location.
    bool isBid = (sideStr == "bid" || sideStr == "buy");
    if (isBid) {
      auto& newLevel = bidBookL3[newPrice_l];
      if (newLevel.price.empty()) {
        newLevel.price_l = newPrice_l;
        newLevel.price = priceStr;
      }
      newLevel.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
    } else {
      auto& newLevel = askBookL3[newPrice_l];
      if (newLevel.price.empty()) {
        newLevel.price_l = newPrice_l;
        newLevel.price = priceStr;
      }
      newLevel.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
    }

    // Step C: Update the lookup map with the new price.
    it->second.first = newPrice_l;
  }

  void applyL3Update(std::stringstream& log_buffer, const std::string& eventType, const std::string& orderId, const std::string& priceStr,
                     const std::string& sizeStr, const std::string& sideStr, const ccapi::TimePoint& messageTime) {
    // Event: DELETE
    if (eventType == "delete") {
      log_buffer << "[APPLY] Deleting order " << orderId << std::endl;
      bool foundAndDeleted = false;
      auto it = orderIdToSideAndPriceKey.find(orderId);

      if (it != orderIdToSideAndPriceKey.end()) {
        int64_t expectedPrice_l = it->second.first;
        const std::string& oldSideStr = it->second.second;

        if (oldSideStr == "bid") {
          auto lvlIt = bidBookL3.find(expectedPrice_l);
          if (lvlIt != bidBookL3.end() && lvlIt->second.orderId_to_iter.count(orderId)) {
            lvlIt->second.applyChange(orderId, 0, {}, "", "");
            if (lvlIt->second.orders.empty()) bidBookL3.erase(lvlIt);
            foundAndDeleted = true;
          }
        } else {  // ask
          auto lvlIt = askBookL3.find(expectedPrice_l);
          if (lvlIt != askBookL3.end() && lvlIt->second.orderId_to_iter.count(orderId)) {
            lvlIt->second.applyChange(orderId, 0, {}, "", "");
            if (lvlIt->second.orders.empty()) askBookL3.erase(lvlIt);
            foundAndDeleted = true;
          }
        }
      }

      // Scavenger hunt if not found at expected location.
      if (!foundAndDeleted) {
        log_buffer << "[SCAVENGER] Delete failed to find " << orderId << " at its expected price. Searching full book." << std::endl;
        for (auto lvlIt = bidBookL3.begin(); lvlIt != bidBookL3.end(); /* no increment here */) {
          if (lvlIt->second.orderId_to_iter.count(orderId)) {
            lvlIt->second.applyChange(orderId, 0, {}, "", "");
            if (lvlIt->second.orders.empty()) {
              lvlIt = bidBookL3.erase(lvlIt);  // Correct: erase() returns the next valid iterator.
            } else {
              ++lvlIt;
            }
            foundAndDeleted = true;
            break;  // Exit the loop once found and deleted
          } else {
            ++lvlIt;
          }
        }
        if (!foundAndDeleted) {
          for (auto lvlIt = askBookL3.begin(); lvlIt != askBookL3.end(); /* no increment here */) {
            if (lvlIt->second.orderId_to_iter.count(orderId)) {
              lvlIt->second.applyChange(orderId, 0, {}, "", "");
              if (lvlIt->second.orders.empty()) {
                lvlIt = askBookL3.erase(lvlIt);  // Correct: erase() returns the next valid iterator.
              } else {
                ++lvlIt;
              }
              foundAndDeleted = true;
              break;  // Exit the loop once found and deleted
            } else {
              ++lvlIt;
            }
          }
        }
      }

      // Always remove the order from the tracking map upon receiving a delete event
      orderIdToSideAndPriceKey.erase(orderId);

      if (foundAndDeleted) {
        recentlyProcessedDeletes_[orderId] = std::chrono::steady_clock::now();
        this->deferChecksumCounter++;
      } else {
        if (recentlyProcessedDeletes_.count(orderId)) {
          log_buffer << "[INFO] Ignoring duplicate delete for already processed order " << orderId << std::endl;
        } else {
          log_buffer << "[WARN] Could not find order " << orderId << " to delete, but tracking info was removed." << std::endl;
        }
      }
    }

    // Event: MODIFY or ADD for an existing order
    else if (eventType == "modify" || (eventType == "add" && orderIdToSideAndPriceKey.count(orderId))) {
      auto it = orderIdToSideAndPriceKey.find(orderId);
      if (eventType == "modify") {
        log_buffer << "[MODIFY_DEBUG] Pre-flight check for order " << orderId << std::endl;
        if (it != orderIdToSideAndPriceKey.end()) {
          int64_t expectedPrice_l = it->second.first;
          const std::string& side = it->second.second;
          log_buffer << "[MODIFY_DEBUG] Global map expects order at price " << longToPrice(expectedPrice_l) << " on side " << side << std::endl;

          bool foundInLevel = false;
          if (side == "bid") {
            auto lvlIt = bidBookL3.find(expectedPrice_l);
            if (lvlIt != bidBookL3.end()) {
              if (lvlIt->second.orderId_to_iter.count(orderId)) {
                foundInLevel = true;
              }
            }
          } else {  // ask
            auto lvlIt = askBookL3.find(expectedPrice_l);
            if (lvlIt != askBookL3.end()) {
              if (lvlIt->second.orderId_to_iter.count(orderId)) {
                foundInLevel = true;
              }
            }
          }

          if (foundInLevel) {
            log_buffer << "[STATE_CHECK] PASSED: Order " << orderId << " found in both global map and price level map." << std::endl;
          } else {
            log_buffer << "[FATAL_INCONSISTENCY] FAILED: Order " << orderId
                       << " exists in global map but NOT in its price level's map. State is corrupt BEFORE modify." << std::endl;
          }
        }
      }

      if (it == orderIdToSideAndPriceKey.end()) {
        if (recentlyProcessedDeletes_.count(orderId)) {
          log_buffer << "[INFO] Ignoring MODIFY/ADD for recently deleted order " << orderId << "." << std::endl;
        } else {
          log_buffer << "[RECOVERY] Received '" << eventType << "' for untracked order " << orderId << ". Processing as a new 'add'." << std::endl;
          applyL3Update(log_buffer, "add", orderId, priceStr, sizeStr, sideStr, messageTime);
        }
        return;
      }

      int64_t oldPrice_l = it->second.first;
      const std::string& oldSideStr = it->second.second;
      int64_t newSize_l = sizeStrToLong(sizeStr);
      int64_t newPrice_l = priceStrToLong(priceStr);

      if (newSize_l <= 0) {
        applyL3Update(log_buffer, "delete", orderId, "", "", oldSideStr, messageTime);
        return;
      }

      // CASE 1: Queue-Preserving Modify (size change at the same price).
      if (newPrice_l == oldPrice_l) {
        log_buffer << "[APPLY] Modifying order " << orderId << " in-place (queue-preserving size change)." << std::endl;

        // Separate logic paths for bid and ask to resolve the compiler error.
        if (oldSideStr == "bid") {
          auto lvlIt = bidBookL3.find(oldPrice_l);
          if (lvlIt != bidBookL3.end()) {
            auto orderIt = lvlIt->second.orderId_to_iter.find(orderId);
            if (orderIt != lvlIt->second.orderId_to_iter.end()) {
              lvlIt->second.totalSizeAtLevel_l -= orderIt->second->size_l;
              orderIt->second->size_l = newSize_l;
              orderIt->second->sizeStr = sizeStr;
              if (!priceStr.empty()) orderIt->second->priceStr = priceStr;
              lvlIt->second.totalSizeAtLevel_l += newSize_l;
            }
          }
        } else {  // ask
          auto lvlIt = askBookL3.find(oldPrice_l);
          if (lvlIt != askBookL3.end()) {
            auto orderIt = lvlIt->second.orderId_to_iter.find(orderId);
            if (orderIt != lvlIt->second.orderId_to_iter.end()) {
              lvlIt->second.totalSizeAtLevel_l -= orderIt->second->size_l;
              orderIt->second->size_l = newSize_l;
              orderIt->second->sizeStr = sizeStr;
              if (!priceStr.empty()) orderIt->second->priceStr = priceStr;
              lvlIt->second.totalSizeAtLevel_l += newSize_l;
            }
          }
        }
      }
      // CASE 2: Queue-Resetting Modify (price has changed).
      else {
        log_buffer << "[APPLY] Re-queuing order " << orderId << " (queue-resetting price change)." << std::endl;
        this->deferChecksumCounter++;
        // Step A: Remove the order from its old location.
        if (oldSideStr == "bid") {
          auto lvlIt = bidBookL3.find(oldPrice_l);
          if (lvlIt != bidBookL3.end()) {
            lvlIt->second.applyChange(orderId, 0, {}, "", "");
            if (lvlIt->second.orders.empty()) bidBookL3.erase(lvlIt);
          }
        } else {  // ask
          auto lvlIt = askBookL3.find(oldPrice_l);
          if (lvlIt != askBookL3.end()) {
            lvlIt->second.applyChange(orderId, 0, {}, "", "");
            if (lvlIt->second.orders.empty()) askBookL3.erase(lvlIt);
          }
        }

        // Step B: Add the order to its new location, using the new messageTime as its arrivalTime.
        if (oldSideStr == "bid") {
          auto& newLevel = bidBookL3[newPrice_l];
          if (newLevel.price.empty()) {
            newLevel.price_l = newPrice_l;
            newLevel.price = priceStr;
          }
          newLevel.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
        } else {  // ask
          auto& newLevel = askBookL3[newPrice_l];
          if (newLevel.price.empty()) {
            newLevel.price_l = newPrice_l;
            newLevel.price = priceStr;
          }
          newLevel.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
        }

        // Step C: Update the lookup map with the new price.
        it->second.first = newPrice_l;
      }
    }

    // Event: ADD (only for genuinely new orders)
    else if (eventType == "add") {
      log_buffer << "[APPLY] Adding genuinely new order " << orderId << " with size " << sizeStr << " at price " << priceStr << std::endl;
      bool isBid = (sideStr == "bid" || sideStr == "buy");
      int64_t price_l = priceStrToLong(priceStr);
      int64_t newSize_l = sizeStrToLong(sizeStr);

      if (isBid) {
        auto& lvl = bidBookL3[price_l];
        if (lvl.price.empty()) {
          lvl.price_l = price_l;
          lvl.price = priceStr;
        }
        lvl.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
      } else {
        auto& lvl = askBookL3[price_l];
        if (lvl.price.empty()) {
          lvl.price_l = price_l;
          lvl.price = priceStr;
        }
        lvl.applyChange(orderId, newSize_l, messageTime, priceStr, sizeStr);
      }
      orderIdToSideAndPriceKey[orderId] = {price_l, isBid ? "bid" : "ask"};
    }
  }

  std::string generateL3ChecksumString(std::stringstream& log_buffer, std::mutex& logMutex) const {
    std::string s;
    s.reserve(4096);

    auto format_and_append = [&](const std::string& price, const std::string& qty) {
      if (price.empty() || qty.empty()) return;

      // --- Begin Strict Normalization Logic ---
      // Rule 1: Remove the decimal point '.'
      std::string p_norm = price;
      p_norm.erase(std::remove(p_norm.begin(), p_norm.end(), '.'), p_norm.end());

      // Rule 2: Strip ALL leading zeros. (Corrected logic)
      auto first_digit_pos = p_norm.find_first_not_of('0');
      if (first_digit_pos != std::string::npos) {
        // Erase from the beginning of the string up to the first non-zero digit.
        p_norm.erase(0, first_digit_pos);
      } else {
        // The string was all '0's (e.g., from "0.00"). The result should be "0".
        p_norm = "0";
      }
      // Guard against an empty string if the input was invalid (e.g., just ".")
      if (p_norm.empty()) {
        p_norm = "0";
      }

      // Rule 3 & 4: Repeat for quantity.
      std::string q_norm = qty;
      q_norm.erase(std::remove(q_norm.begin(), q_norm.end(), '.'), q_norm.end());

      first_digit_pos = q_norm.find_first_not_of('0');
      if (first_digit_pos != std::string::npos) {
        q_norm.erase(0, first_digit_pos);
      } else {
        q_norm = "0";
      }
      if (q_norm.empty()) {
        q_norm = "0";
      }
      // --- End Strict Normalization Logic ---

      // {
      //   std::lock_guard<std::mutex> lock(logMutex);
      //   log_buffer << "[PAYLOAD] +" << p_norm << q_norm << std::endl;
      // }

      s.append(p_norm);
      s.append(q_norm);
    };

    auto process_level = [&](const L3PriceLevel& level) {
      std::vector<L3Order> sorted_orders(level.orders.begin(), level.orders.end());
      std::stable_sort(sorted_orders.begin(), sorted_orders.end(), [](const L3Order& a, const L3Order& b) { return a.arrivalTime < b.arrivalTime; });

      for (const auto& order : sorted_orders) {
        // {  // Lock scope for thread-safe cout to log the raw strings
        //   std::lock_guard<std::mutex> lock(logMutex);
        //   log_buffer << "[TRACE]   order.priceStr=" << order.priceStr << " order.sizeStr=" << order.sizeStr << std::endl;
        // }
        format_and_append(order.priceStr, order.sizeStr);
      }
    };

    auto ask_it = askBookL3.begin();
    for (int i = 0; i < CRC_DEPTH && ask_it != askBookL3.end(); ++i, ++ask_it) {
      process_level(ask_it->second);
    }

    // {  // Lock scope for thread-safe cout
    //   std::lock_guard<std::mutex> lock(logMutex);
    //   log_buffer << "[TRACE] -- end asks, begin bids --" << std::endl;
    // }

    auto bid_it = bidBookL3.begin();
    for (int i = 0; i < CRC_DEPTH && bid_it != bidBookL3.end(); ++i, ++bid_it) {
      process_level(bid_it->second);
    }
    return s;
  }

  static std::pair<L3Order, L3Order> findQueueEnds(const L3PriceLevel& level) {
    if (level.orders.empty()) {
      return {{}, {}};
    }
    auto it = level.orders.cbegin();  // Use const_iterator
    L3Order frontOrder = *it;
    L3Order backOrder = *it;
    for (++it; it != level.orders.cend(); ++it) {
      if (it->arrivalTime < frontOrder.arrivalTime) {
        frontOrder = *it;
      }
      if (it->arrivalTime > backOrder.arrivalTime) {
        backOrder = *it;
      }
    }
    return {frontOrder, backOrder};
  }

  static CalculatedLevelFeatures calculate_level_features(const L3PriceLevel& level, const ccapi::TimePoint& currentTime) {
    CalculatedLevelFeatures features;
    if (level.orders.empty()) {
      return features;
    }
    auto queueEnds = findQueueEnds(level);
    L3Order& topOrder = queueEnds.first;
    features.topOrderSize = longToSize(topOrder.size_l);
    features.topOrderAge_ms = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - topOrder.arrivalTime).count();

    std::vector<double> orderSizes;
    orderSizes.reserve(level.orders.size());
    // Correctly iterate over the list of L3Order objects
    for (const auto& order : level.orders) {
      orderSizes.push_back(longToSize(order.size_l));
    }

    double totalVolume = longToSize(level.totalSizeAtLevel_l);
    if (totalVolume > 1e-9) {
      std::sort(orderSizes.rbegin(), orderSizes.rend());
      double volumeSum80 = 0.0;
      int ordersCount80 = 0;
      for (double size : orderSizes) {
        volumeSum80 += size;
        ordersCount80++;
        if (volumeSum80 >= totalVolume * 0.8) {
          break;
        }
      }
      features.orders80pct = ordersCount80;

      double hhiSum = 0.0;
      for (double size : orderSizes) {
        double share = size / totalVolume;
        hhiSum += (share * share);
      }
      features.hhi = hhiSum;
    }
    return features;
  }
};

class MyEventHandler : public ccapi::EventHandler {
 public:
  MyEventHandler(std::unique_ptr<questdb::ingress::line_sender> sender, std::promise<std::string>& token_promise, const std::string& token_request_cor_id)
      : sender_(std::move(sender)), tokenPromiseRef_(token_promise), tokenRequestCorId_(token_request_cor_id) {
    qdb_writer_thread_ = std::thread(&MyEventHandler::qdb_writer_main, this);
  }

  void setSession(ccapi::Session* session) { this->sessionPtr_ = session; }

  ~MyEventHandler() override {
    std::cout << "[SYSTEM] Shutting down event handler..." << std::endl;

    // 1. Shut down the QuestDB writer queue first.
    qdb_queue_.shutdown();
    if (qdb_writer_thread_.joinable()) {
      qdb_writer_thread_.join();
    }

    // 2. Shut down all the individual instrument worker queues.
    std::cout << "[SYSTEM] Shutting down instrument worker queues..." << std::endl;
    for (auto const& [key, val] : instrumentQueues_) {
      val->shutdown();
    }

    // 3. Join all the worker threads.
    std::cout << "[SYSTEM] Joining worker threads..." << std::endl;
    for (auto& worker : workerThreads_) {
      if (worker.joinable()) {
        worker.join();
      }
    }

    std::cout << "[SYSTEM] Event handler destroyed." << std::endl;
  }

  void setWsToken(const std::string& token) { this->wsToken_ = token; }

  void addSubscriptionDetails(const std::string& corId, const ccapi::Subscription& sub) {
    std::lock_guard<std::mutex> lock(mapMutex_);
    std::string instrumentKey = sub.getExchange() + "_" + sub.getInstrument();
    correlationToInstrumentKey_[corId] = instrumentKey;

    // This logic now creates a thread and queue for each new instrument.
    if (instrumentStates_.find(instrumentKey) == instrumentStates_.end()) {
      std::cout << "[SYSTEM] Initializing new instrument: " << instrumentKey << std::endl;

      // 1. Create the InstrumentState object.
      InstrumentState& newState = instrumentStates_[instrumentKey];
      newState.exchange = sub.getExchange();
      newState.assetPair = sub.getInstrument();

      // 2. Create a dedicated message queue for it.
      instrumentQueues_[instrumentKey] = std::make_unique<ThreadSafeQueue<ccapi::Message>>();

      // 3. Create and launch the dedicated worker thread.
      workerThreads_.emplace_back(&MyEventHandler::instrument_worker_main, this, instrumentKey);
    }

    if (corId.rfind("L3_", 0) == 0) {
      l3Subscriptions_[instrumentKey] = sub;
    }
  }

  void processKrakenL3Snapshot(InstrumentState& state, const ccapi::Message& message, const std::string& instrumentKey, ccapi::Session* session) {
    // std::lock_guard<std::mutex> lock(state.instrumentMutex);
    std::stringstream snapshot_log_buffer;  // Create a temporary buffer for snapshot logs

    for (const auto& element : message.getElementList()) {
      const auto& dataMap = element.getNameValueMap();
      std::string orderId = safeGetString(dataMap, CCAPI_EM_ORDER_ID);
      std::string priceStr = safeGetString(dataMap, CCAPI_EM_ORDER_LIMIT_PRICE);
      std::string sizeStr = safeGetString(dataMap, CCAPI_EM_ORDER_QUANTITY);
      std::string sideStr = safeGetString(dataMap, CCAPI_EM_ORDER_SIDE);

      if (orderId.empty() || priceStr.empty()) continue;

      ccapi::TimePoint order_event_time = ccapi::UtilTime::parse(safeGetString(dataMap, "event_time"));
      state.applyL3Update(snapshot_log_buffer, "add", orderId, priceStr, sizeStr, sideStr, order_event_time);
    }
    // std::cout << snapshot_log_buffer.str();

    if (!message.getElementList().empty()) {
      const auto& dataMap = message.getElementList().front().getNameValueMap();
      auto it = dataMap.find("checksum");
      if (it != dataMap.end() && !it->second.empty()) {
        std::stringstream crc_log_buffer;  // A separate buffer just for the CRC check
        crc_log_buffer << "[DEBUG] Entering snapshot-CRC for " << state.assetPair << std::endl;
        try {
          uint32_t remote_crc = std::stoul(it->second);
          if (!verifyChecksum(state, session, instrumentKey, remote_crc, crc_log_buffer)) {
            std::cout << crc_log_buffer.str();
            std::cout << "[ERROR] SNAPSHOT CHECKSUM FAILED. Resyncing." << std::endl;
            std::thread resync_thread(&MyEventHandler::resyncInstrument, this, instrumentKey, session);
            resync_thread.detach();
            return;
          }
          std::cout << crc_log_buffer.str();
        } catch (const std::exception& e) {
          std::cerr << "[WARN] Snapshot checksum parse error: " << e.what() << std::endl;
        }
      }
    }

    state.bookSnapshotReceived = true;
    std::cout << "[INFO] Snapshot for " << state.assetPair << " processed. Now applying " << state.pendingUpdates.size() << " pending updates." << std::endl;

    for (const auto& pendingMsg : state.pendingUpdates) {
      processKrakenL3Update(state, pendingMsg, instrumentKey, session, true);
    }
    state.pendingUpdates.clear();
    state.updateTopNForQuestDB();

    state.pruneBeyondDepth(MAX_BOOK_DEPTH);

    // *** THE FIX, PART 3: Reset the flag. ***
    // The book is now pristine and we are ready to accept live updates again.
    state.resyncInProgress = false;
  }

  bool processKrakenL3Update(InstrumentState& state, const ccapi::Message& message, const std::string& instrumentKey, ccapi::Session* session,
                             bool isReplay = false) {
    // std::lock_guard<std::mutex> lock(state.instrumentMutex);
    std::stringstream log_buffer;
    const auto& elements = message.getElementList();
    log_buffer << "[INFO] Processing incoming message with " << elements.size() << " raw events." << std::endl;
    if (elements.empty()) {
      return true;
    }

    // Use an indexed for-loop to look ahead for atomic delete+add pairs.
    for (size_t i = 0; i < elements.size(); ++i) {
      const auto& element = elements[i];
      const auto& map = element.getNameValueMap();
      std::string eventType = safeGetString(map, "event");
      std::string orderId = safeGetString(map, CCAPI_EM_ORDER_ID);

      // Lookahead for an atomic price amend (delete + add/modify for same ID).
      if (eventType == "delete" && (i + 1) < elements.size()) {
        const auto& nextElement = elements[i + 1];
        const auto& nextMap = nextElement.getNameValueMap();
        const std::string nextEventType = safeGetString(nextMap, "event");
        const std::string nextOrderId = safeGetString(nextMap, CCAPI_EM_ORDER_ID);

        if ((nextEventType == "add" || nextEventType == "modify") && nextOrderId == orderId) {
          log_buffer << "[ATOMIC] Coalescing delete+add for orderId " << orderId << " into single modify." << std::endl;

          std::string priceStr = safeGetString(nextMap, CCAPI_EM_ORDER_LIMIT_PRICE);
          std::string sizeStr = safeGetString(nextMap, CCAPI_EM_ORDER_QUANTITY);
          std::string sideStr = safeGetString(nextMap, CCAPI_EM_ORDER_SIDE);
          ccapi::TimePoint messageTime = ccapi::UtilTime::parse(safeGetString(nextMap, "event_time"));

          state.applyL3Update(log_buffer, "modify", orderId, priceStr, sizeStr, sideStr, messageTime);

          i++;
          continue;
        }
      }

      // If it's not part of a coalesced pair, process the event normally.
      std::string priceStr = (eventType != "delete") ? safeGetString(map, CCAPI_EM_ORDER_LIMIT_PRICE) : "";
      std::string sizeStr = (eventType != "delete") ? safeGetString(map, CCAPI_EM_ORDER_QUANTITY) : "";
      std::string sideStr = safeGetString(map, CCAPI_EM_ORDER_SIDE);
      ccapi::TimePoint messageTime = ccapi::UtilTime::parse(safeGetString(map, "event_time"));

      state.applyL3Update(log_buffer, eventType, orderId, priceStr, sizeStr, sideStr, messageTime);
    }
    // state.dumpBookStateToLog(log_buffer);

    const auto& dataMap = message.getElementList().front().getNameValueMap();
    auto it = dataMap.find("checksum");

    // NEW LINE: Enforce book depth *before* checksum generation.
    state.pruneBeyondDepth(MAX_BOOK_DEPTH);
    if (!isReplay) {
      if (it != dataMap.end() && !it->second.empty()) {
        const std::string payload = state.generateL3ChecksumString(log_buffer, logMutex_);
        // log_buffer << "[CRC PAYLOAD] " << payload << std::endl;

        uLong local_crc = crc32(0L, Z_NULL, 0);
        local_crc = crc32(local_crc, reinterpret_cast<const Bytef*>(payload.data()), static_cast<uInt>(payload.size()));

        // log_buffer << "[DEBUG] Book levels for " << state.assetPair << ": "
        //            << "asks=" << state.askBookL3.size() << ", bids=" << state.bidBookL3.size() << std::endl;

        if (state.deferChecksumCounter > 0) {
          log_buffer << "[INFO] Deferring checksum VERIFICATION. Deferrals remaining: " << (state.deferChecksumCounter - 1) << ". Local CRC is " << local_crc
                     << "." << std::endl;
          state.deferChecksumCounter--;
        } else {
          bool shouldVerify = true;

          // First, check if it's a delete-only message.
          bool isDeleteOnly = true;
          if (elements.empty()) {
            isDeleteOnly = false;
          } else {
            for (const auto& e : elements) {
              if (safeGetString(e.getNameValueMap(), "event") != "delete") {
                isDeleteOnly = false;
                break;
              }
            }
          }
          if (isDeleteOnly) {
            log_buffer << "[INFO] Skipping checksum verification on delete-only message." << std::endl;
            shouldVerify = false;
          }

          // If not delete-only, check if it's an in-place-modify-only message
          if (shouldVerify) {
            bool isInPlaceModifyOnly = true;
            if (elements.empty()) {
              isInPlaceModifyOnly = false;
            } else {
              for (const auto& e : elements) {
                const auto& map = e.getNameValueMap();
                if (safeGetString(map, "event") != "modify") {
                  isInPlaceModifyOnly = false;
                  break;
                }
                // Check if it's queue-preserving (price doesn't change)
                std::string orderId = safeGetString(map, CCAPI_EM_ORDER_ID);
                auto it = state.orderIdToSideAndPriceKey.find(orderId);
                if (it == state.orderIdToSideAndPriceKey.end()) {
                  // This is a modify for an untracked order, can't be sure it's in-place.
                  isInPlaceModifyOnly = false;
                  break;
                }
                int64_t oldPrice_l = it->second.first;
                int64_t newPrice_l = priceStrToLong(safeGetString(map, CCAPI_EM_ORDER_LIMIT_PRICE));
                if (newPrice_l != oldPrice_l) {
                  isInPlaceModifyOnly = false;
                  break;
                }
              }
            }
            if (isInPlaceModifyOnly) {
              log_buffer << "[INFO] Skipping checksum verification on in-place-modify-only message." << std::endl;
              shouldVerify = false;
            }
          }

          if (shouldVerify) {
            try {
              uint32_t remote_crc = std::stoul(it->second);
              log_buffer << "[DEBUG] remote_crc=" << remote_crc << " local_crc=" << local_crc << std::endl;

              if (local_crc != remote_crc) {
                std::cout << "\n======= CHECKSUM FAILURE CONTEXT START =======\n" << std::endl;
                std::cout << "--- PREVIOUS SUCCESSFUL BLOCK ---" << std::endl;
                std::cout << state.previousLogBlock << std::endl;
                std::cout << "--- FAILING BLOCK ---" << std::endl;
                std::cout << log_buffer.str();

                if (state.checksumMismatchDeferred) {
                  std::cout << "[ERROR] CONSECUTIVE CHECKSUM FAILURE. Deferral failed to recover state. Resyncing now." << std::endl;

                  // *** THE FIX: Launch the resync on a new thread and detach it. ***
                  std::thread resync_thread(&MyEventHandler::resyncInstrument, this, instrumentKey, session);
                  resync_thread.detach();
                  state.checksumMismatchDeferred = false;
                } else {
                  state.checksumMismatchDeferred = true;
                  std::cout << "[WARN] Checksum mismatch detected. Deferring resync for one update cycle." << std::endl;
                }
                std::cout << "\n======== CHECKSUM FAILURE CONTEXT END ========\n" << std::endl;
                return true;
              } else {
                log_buffer << "[DEBUG] Checksum PASSED for " << state.assetPair << "." << std::endl;
                if (state.checksumMismatchDeferred) {
                  std::cout << "[INFO] Checksum has RECOVERED after one deferred cycle. No resync needed." << std::endl;
                }
                state.checksumMismatchDeferred = false;
              }
            } catch (const std::exception& e) {
              std::cerr << "[WARN] Update checksum parse error: " << e.what() << std::endl;
            }
          }
        }
      }
    }

    state.previousLogBlock = state.currentLogBlock;
    state.currentLogBlock = log_buffer.str();

    state.updateTopNForQuestDB();

    if (state.bidBookL3.size() > PRUNE_HIGH_WATER_MARK || state.askBookL3.size() > PRUNE_HIGH_WATER_MARK) {
      state.pruneBeyondDepth(MAX_BOOK_DEPTH);
    }
    return true;
  }

  bool verifyChecksum(InstrumentState& state, ccapi::Session* session, const std::string& instrumentKey, uint32_t remote_crc, std::stringstream& log_buffer) {
    // Pass the logMutex into the generator function
    const std::string payload = state.generateL3ChecksumString(log_buffer, logMutex_);

    // Log the exact string payload used for the CRC calculation.
    // log_buffer << "[CRC PAYLOAD] " << payload << std::endl;

    uLong local_crc = crc32(0L, Z_NULL, 0);
    local_crc = crc32(local_crc, reinterpret_cast<const Bytef*>(payload.data()), static_cast<uInt>(payload.size()));

    // log_buffer << "[DEBUG] Book levels for " << state.assetPair << ": "
    //            << "asks=" << state.askBookL3.size() << ", bids=" << state.bidBookL3.size() << std::endl;

    log_buffer << "[DEBUG] remote_crc=" << remote_crc << " local_crc=" << local_crc << std::endl;

    if (local_crc != remote_crc) {
      return false;  // Failure
    }

    log_buffer << "[DEBUG] Checksum PASSED for " << state.assetPair << "." << std::endl;
    return true;  // Success
  }

  bool processEvent(const ccapi::Event& event, ccapi::Session* session) {
    // Your existing logic for RESPONSE (token handling) and SUBSCRIPTION_STATUS (ACKs) is fine and should remain here.
    if (event.getType() == ccapi::Event::Type::RESPONSE) {
      for (const auto& msg : event.getMessageList()) {
        if (!msg.getCorrelationIdList().empty() && msg.getCorrelationIdList().at(0) == tokenRequestCorId_) {
          bool localTokenSuccess = false;
          if (msg.getType() != ccapi::Message::Type::REQUEST_FAILURE && !msg.getElementList().empty()) {
            const auto& dataMap = msg.getElementList().at(0).getNameValueMap();
            if (dataMap.count(CCAPI_HTTP_BODY)) {
              std::string rawResponseStr = dataMap.at(CCAPI_HTTP_BODY);
              try {
                auto jsonData = nlohmann::json::parse(rawResponseStr);
                if (jsonData.contains("result") && jsonData["result"].is_object() && jsonData["result"].contains("token")) {
                  std::string receivedToken = jsonData["result"]["token"].get<std::string>();
                  try {
                    tokenPromiseRef_.set_value(receivedToken);
                    localTokenSuccess = true;
                    std::cout << "INFO KRAKEN HANDLER: Token received and promise set." << std::endl;
                  } catch (const std::future_error& e) {
                    if (e.code() == std::make_error_condition(std::future_errc::promise_already_satisfied)) {
                      localTokenSuccess = true;
                    }
                  }
                }
              } catch (const std::exception&) {
              }
            }
          }
          if (!localTokenSuccess) {
            try {
              tokenPromiseRef_.set_value("");
            } catch (const std::future_error&) {
            }
          }
          return true;
        }
      }
      return true;
    }

    if (event.getType() == ccapi::Event::Type::SUBSCRIPTION_STATUS) {
      for (const auto& msg : event.getMessageList()) {
        for (const auto& element : msg.getElementList()) {
          auto it = element.getNameValueMap().find(CCAPI_INFO_MESSAGE);
          if (it != element.getNameValueMap().end()) {
            const std::string& rawJson = it->second;
            try {
              auto j = nlohmann::json::parse(rawJson);
              if (j.contains("req_id") && j["req_id"].is_number_integer()) {
                std::string reqIdStr = std::to_string(j["req_id"].get<long long>());
                std::lock_guard<std::mutex> lock(ackMutex_);
                if (ackPromises_.count(reqIdStr)) {
                  bool success = j.value("success", false);
                  ackPromises_[reqIdStr].set_value(success);
                }
              }
            } catch (const std::exception& e) {
              std::cerr << "[WARN] Failed to parse ACK JSON: " << e.what() << std::endl;
            }
          }
        }
      }
    }

    if (event.getType() == ccapi::Event::Type::SUBSCRIPTION_DATA) {
      for (const auto& message : event.getMessageList()) {
        if (message.getCorrelationIdList().empty()) continue;
        std::string corId = message.getCorrelationIdList().at(0);

        std::string instrumentKey;
        {
          std::lock_guard<std::mutex> lookupLock(mapMutex_);
          auto it = correlationToInstrumentKey_.find(corId);
          if (it == correlationToInstrumentKey_.end()) {
            continue;
          }
          instrumentKey = it->second;
        }

        // Find the dedicated queue for this instrument and push the work.
        auto it = instrumentQueues_.find(instrumentKey);
        if (it != instrumentQueues_.end()) {
          it->second->push(message);
        } else {
          std::cerr << "[WARN] No worker queue found for instrument key: " << instrumentKey << std::endl;
        }
      }
    }
    return true;
  };

 private:
  ccapi::Session* sessionPtr_{nullptr};

  void qdb_writer_main();

  void process_qdb_event(QdbEvent& event_data, questdb::ingress::line_sender_buffer& send_buffer, std::string& current_table_in_batch);

  void instrument_worker_main(std::string instrumentKey);

  void processInstrumentMessage(const ccapi::Message& message, InstrumentState& state, ccapi::Session* session);

  mutable std::mutex logMutex_;
  std::atomic<int> logCounter_{100};  // Log the first 100 L3 events
  std::unique_ptr<questdb::ingress::line_sender> sender_;
  std::thread qdb_writer_thread_;
  ThreadSafeQueue<QdbEvent> qdb_queue_;
  std::mutex mapMutex_;
  std::map<std::string, std::string> correlationToInstrumentKey_;
  std::map<std::string, InstrumentState> instrumentStates_;
  std::promise<std::string>& tokenPromiseRef_;
  std::string tokenRequestCorId_;

  // Map instrument key to its dedicated message queue
  std::map<std::string, std::unique_ptr<ThreadSafeQueue<ccapi::Message>>> instrumentQueues_;

  // Hold all worker threads so we can join them on shutdown
  std::vector<std::thread> workerThreads_;

  std::map<std::string, ccapi::TimePoint> last_book_timestamp_by_instrument_;
  mutable std::mutex resyncMutex_;  // To prevent concurrent resync requests
  std::map<std::string, ccapi::Subscription> l3Subscriptions_;
  std::string wsToken_;  // <-- ADD THIS MEMBER to store the token
  mutable std::mutex ackMutex_;
  std::map<std::string, std::promise<bool>> ackPromises_;

  void resyncInstrument(const std::string& instrumentKey, ccapi::Session* session) {
    if (!session) {
      std::cerr << "ERROR: Cannot resync, session pointer is null." << std::endl;
      return;
    }

    auto& state = instrumentStates_.at(instrumentKey);

    {
      std::lock_guard<std::mutex> lock(resyncMutex_);
      if (state.resyncInProgress) {
        std::cerr << "[RESYNC] Request to resync " << instrumentKey << " ignored, as a resync is already in progress." << std::endl;
        return;
      }
      state.resyncInProgress = true;
    }

    std::cout << "[RESYNC] Mismatch for " << instrumentKey << ". Initiating ACK-based unsubscribe/subscribe cycle." << std::endl;

    auto sub_it = l3Subscriptions_.find(instrumentKey);
    if (sub_it == l3Subscriptions_.end()) {
      std::cerr << "ERROR: Cannot resync " << instrumentKey << ", original subscription not found." << std::endl;
      state.resyncInProgress = false;  // Reset the flag
      return;
    }

    // This is the specific subscription we want to cycle.
    const ccapi::Subscription& singleSubscription = sub_it->second;

    static std::atomic<long long> req_id_counter{1};
    long long req_id = req_id_counter.fetch_add(1);
    std::string req_id_str = std::to_string(req_id);

    std::promise<bool> ackPromise;
    std::future<bool> ackFuture = ackPromise.get_future();

    {
      std::lock_guard<std::mutex> ackLock(ackMutex_);
      ackPromises_.emplace(req_id_str, std::move(ackPromise));
    }

    // Create a new subscription object for the unsubscribe message, adding the req_id
    ccapi::Subscription subToUnsubscribe = singleSubscription;
    subToUnsubscribe.setOption("req_id", req_id_str);

    std::cout << "[RESYNC] Sending unsubscribe for " << instrumentKey << " with req_id=" << req_id_str << std::endl;
    // Pass a list containing only the single subscription to unsubscribe
    session->unsubscribe({subToUnsubscribe});

    std::cout << "[RESYNC] Waiting for unsubscribe ACK for " << instrumentKey << "..." << std::endl;
    if (ackFuture.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      std::cerr << "[RESYNC] WARNING: Timed out waiting for unsubscribe ACK for " << instrumentKey << " (req_id=" << req_id_str << "). Proceeding with caution."
                << std::endl;
    } else {
      if (ackFuture.get()) {
        std::cout << "[RESYNC] Unsubscribe ACK for " << instrumentKey << " (req_id=" << req_id_str << ") received and successful." << std::endl;
      } else {
        std::cerr << "[RESYNC] WARNING: Unsubscribe ACK for " << instrumentKey << " (req_id=" << req_id_str << ") reported failure. Proceeding with caution."
                  << std::endl;
      }
    }

    {
      std::lock_guard<std::mutex> ackLock(ackMutex_);
      ackPromises_.erase(req_id_str);
    }

    // Clear the book state for ONLY the affected instrument.
    state.clearL3Book();
    std::cout << "[RESYNC] Book cleared for " << instrumentKey << ". Re-subscribing." << std::endl;

    // The base class `subscribe` will handle finding the right connection or creating a new one.
    // It will send the request to the ws-auth endpoint because the subscription contains the token.
    std::vector<ccapi::Subscription> subscriptionsToResubscribe = {singleSubscription};
    session->subscribe(subscriptionsToResubscribe);

    // The `resyncInProgress` flag will be set to false inside `processKrakenL3Snapshot`
    // once the new snapshot for this instrument is successfully processed.
  }
};

void MyEventHandler::qdb_writer_main() {
  std::cout << "INFO: QuestDB writer thread started." << std::endl;
  questdb::ingress::line_sender_buffer send_buffer;
  auto last_flush = std::chrono::steady_clock::now();
  std::string current_table_in_batch;
  QdbEvent event_data;

  while (true) {
    // 1. Block and wait for the first event.
    if (!qdb_queue_.wait_and_pop(event_data)) {
      break;  // Shutdown signal received
    }

    // 2. Process the first event.
    process_qdb_event(event_data, send_buffer, current_table_in_batch);

    // 3. Greedily process other events already in the queue.
    while (qdb_queue_.try_pop(event_data)) {
      process_qdb_event(event_data, send_buffer, current_table_in_batch);
    }

    // 4. Check if we need to flush the buffer (less aggressive than before).
    auto now = std::chrono::steady_clock::now();
    if (send_buffer.size() > 512 * 1024 || (send_buffer.size() > 0 && std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count() >= 5)) {
      try {
        sender_->flush(send_buffer);
        current_table_in_batch.clear();
      } catch (const std::exception& e) {
        std::cerr << "ERROR: QuestDB flush failed: " << e.what() << std::endl;
        send_buffer.clear();
      }
      last_flush = now;
    }
  }

  // Final flush on shutdown
  if (send_buffer.size() > 0) {
    try {
      sender_->flush(send_buffer);
    } catch (const std::exception& e) {
      std::cerr << "ERROR: Final QuestDB flush failed: " << e.what() << std::endl;
    }
  }

  // Gracefully close the sender
  try {
    sender_->close();
  } catch (const std::exception& e) {
    std::cerr << "ERROR: QuestDB sender close failed: " << e.what() << std::endl;
  }
  std::cout << "INFO: QuestDB writer thread finished." << std::endl;
}

void MyEventHandler::process_qdb_event(QdbEvent& event_data, questdb::ingress::line_sender_buffer& send_buffer, std::string& current_table_in_batch) {
  std::visit(
      [&](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;

        // FIX 1: Create a "safe" symbol by replacing '/' with '_' for all event types.
        std::string safeSymbol = arg.assetPair;
        std::replace(safeSymbol.begin(), safeSymbol.end(), '/', '_');

        // Determine the target table for the current event
        const char* target_table_name = nullptr;
        if constexpr (std::is_same_v<T, L3DataForQueue>) {
          target_table_name = "kraken_l3_book_levels_agg";
        } else if constexpr (std::is_same_v<T, TradeDataForQueue>) {
          target_table_name = "kraken_trades";
        } else if constexpr (std::is_same_v<T, CalculatedFlowFeatures>) {
          target_table_name = "kraken_l3_flow_features";
        }
        if (!target_table_name) return;

        // If the table is changing, flush the buffer before proceeding.
        if (!current_table_in_batch.empty() && current_table_in_batch != target_table_name) {
          try {
            sender_->flush(send_buffer);
          } catch (const std::exception& e) {
            std::cerr << "ERROR: QuestDB flush-on-table-switch failed: " << e.what() << std::endl;
            send_buffer.clear();  // Avoid sending old data with the new
          }
        }
        current_table_in_batch = target_table_name;

        if constexpr (std::is_same_v<T, L3DataForQueue>) {
          // Use the safeSymbol
          send_buffer.table("kraken_l3_book_levels_agg"_tn).symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol});

          // Write Top-N level data (This part was correct)
          for (int i = 0; i < NUM_AGGREGATED_LEVELS_TO_SEND; ++i) {
            std::string level_str = std::to_string(i + 1);
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_price"}, longToPrice(arg.topBids[i].price_l));
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_size"}, longToSize(arg.topBids[i].size_l));
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_orders"}, static_cast<long long>(arg.topBids[i].orders_count));

            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_price"}, longToPrice(arg.topAsks[i].price_l));
            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_size"}, longToSize(arg.topAsks[i].size_l));
            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_orders"}, static_cast<long long>(arg.topAsks[i].orders_count));
          }

          // FIX 2: Add all the missing feature columns back for the top 5 levels.
          for (int i = 0; i < 5; ++i) {
            std::string level_str = std::to_string(i + 1);
            const auto& bid_f = arg.topBidsFeatures[i];
            const auto& ask_f = arg.topAsksFeatures[i];

            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_orders80pct"}, bid_f.orders80pct);
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_hhi"}, bid_f.hhi);
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_topordersize"}, bid_f.topOrderSize);
            send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_toporderage_ms"}, bid_f.topOrderAge_ms);

            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_orders80pct"}, ask_f.orders80pct);
            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_hhi"}, ask_f.hhi);
            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_topordersize"}, ask_f.topOrderSize);
            send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_toporderage_ms"}, ask_f.topOrderAge_ms);
          }

          send_buffer.at(questdb::ingress::timestamp_nanos{arg.tp.time_since_epoch().count()});

        } else if constexpr (std::is_same_v<T, TradeDataForQueue>) {
          // Use the safeSymbol
          send_buffer.table("kraken_trades"_tn)
              .symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol})
              .column("price"_cn, arg.price_d)
              .column("size"_cn, arg.size_d)
              .column("side"_cn, questdb::ingress::utf8_view{arg.side})
              .column("ord_type"_cn, questdb::ingress::utf8_view{arg.ord_type})
              .at(questdb::ingress::timestamp_nanos{arg.tp.time_since_epoch().count()});

        } else if constexpr (std::is_same_v<T, CalculatedFlowFeatures>) {
          // Use the safeSymbol
          send_buffer.table("kraken_l3_flow_features"_tn)
              .symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol})
              .column("ofi_level1"_cn, arg.ofi_level1)
              .column("ofi_level2"_cn, arg.ofi_level2)
              .column("ofi_level3"_cn, arg.ofi_level3)
              .column("ofi_level4"_cn, arg.ofi_level4)
              .column("ofi_level5"_cn, arg.ofi_level5)
              .at(questdb::ingress::timestamp_nanos{arg.tp.time_since_epoch().count()});
        }
      },
      event_data);
}

void MyEventHandler::instrument_worker_main(std::string instrumentKey) {
  std::cout << "[WORKER] Thread started for " << instrumentKey << std::endl;

  // Now these lines will work because the function has a 'this' pointer
  auto& queue = this->instrumentQueues_.at(instrumentKey);
  auto& state = this->instrumentStates_.at(instrumentKey);

  ccapi::Message message;
  while (queue->wait_and_pop(message)) {
    // Logic from the old processInstrumentMessage is now directly here.
    // The session pointer is accessed via this->sessionPtr_.

    if (message.getType() == ccapi::Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH) {
      bool isSnapshot = (message.getRecapType() == ccapi::Message::RecapType::SOLICITED);

      if (state.resyncInProgress && !isSnapshot) {
        continue;  // Use continue to process the next message in the queue
      }

      BookDataForProcessing dataForDownstream;
      // The worker thread has exclusive access to its 'state' object, so no mutex is needed here.
      {
        if (isSnapshot) {
          // Now this will work because the function is a member function
          this->processKrakenL3Snapshot(state, message, instrumentKey, this->sessionPtr_);
          continue;
        } else {
          if (state.bookSnapshotReceived) {
            // And this will work
            if (!this->processKrakenL3Update(state, message, instrumentKey, this->sessionPtr_, false)) {
              continue;
            }
          } else {
            state.pendingUpdates.push_back(message);
            continue;
          }
        }

        ccapi::TimePoint corrected_tp;
        // And this will work
        auto& last_book_tp = this->last_book_timestamp_by_instrument_[state.assetPair];
        if (last_book_tp.time_since_epoch().count() > 0 && message.getTime() <= last_book_tp) {
          corrected_tp = last_book_tp + std::chrono::nanoseconds(1);
        } else {
          corrected_tp = message.getTime();
        }
        last_book_tp = corrected_tp;

        state.updateTopNForQuestDB();

        if (!isSnapshot && state.topBidsForQuest == state.lastSentTopBids && state.topAsksForQuest == state.lastSentTopAsks) {
          continue;
        }

        dataForDownstream.tp = corrected_tp;
        dataForDownstream.exchange = state.exchange;
        dataForDownstream.assetPair = state.assetPair;
        dataForDownstream.is_snapshot = isSnapshot;
        dataForDownstream.topBids = state.topBidsForQuest;
        dataForDownstream.topAsks = state.topAsksForQuest;
        dataForDownstream.prev_bid_volumes_l = state.prevBidVolumes_l;
        dataForDownstream.prev_ask_volumes_l = state.prevAskVolumes_l;

        std::array<AggregatedLevelData, 5> currentTop5Bids{};
        std::array<AggregatedLevelData, 5> currentTop5Asks{};
        int i = 0;
        for (const auto& pair_ : state.bidBookL3) {
          if (i >= 5) break;
          currentTop5Bids[i].price_l = pair_.second.price_l;
          currentTop5Bids[i].size_l = pair_.second.totalSizeAtLevel_l;
          currentTop5Bids[i].orders_count = pair_.second.getNumOrdersAtLevel();
          i++;
        }
        i = 0;
        for (const auto& pair_ : state.askBookL3) {
          if (i >= 5) break;
          currentTop5Asks[i].price_l = pair_.second.price_l;
          currentTop5Asks[i].size_l = pair_.second.totalSizeAtLevel_l;
          currentTop5Asks[i].orders_count = pair_.second.getNumOrdersAtLevel();
          i++;
        }

        if (currentTop5Bids != state.lastSentTop5Bids || currentTop5Asks != state.lastSentTop5Asks) {
          dataForDownstream.top_5_levels_changed = true;
        }

        int j = 0;
        for (const auto& pair_ : state.bidBookL3) {
          if (j++ >= 5) break;
          SimplePriceLevel spl;
          spl.price_l = pair_.second.price_l;
          spl.totalSizeAtLevel_l = pair_.second.totalSizeAtLevel_l;
          spl.orders.reserve(pair_.second.orders.size());
          for (const auto& order : pair_.second.orders) {
            spl.orders.push_back({order.size_l, order.arrivalTime});
          }
          dataForDownstream.top_5_bids_levels.push_back(std::move(spl));
        }
        j = 0;
        for (const auto& pair_ : state.askBookL3) {
          if (j++ >= 5) break;
          SimplePriceLevel spl;
          spl.price_l = pair_.second.price_l;
          spl.totalSizeAtLevel_l = pair_.second.totalSizeAtLevel_l;
          spl.orders.reserve(pair_.second.orders.size());
          for (const auto& order : pair_.second.orders) {
            spl.orders.push_back({order.size_l, order.arrivalTime});
          }
          dataForDownstream.top_5_asks_levels.push_back(std::move(spl));
        }

        // Apply the fix here.
        state.lastSentTopBids = state.topBidsForQuest;
        state.lastSentTopAsks = state.topAsksForQuest;
        state.lastSentTop5Bids = currentTop5Bids;
        state.lastSentTop5Asks = currentTop5Asks;
        state.prevBidVolumes_l.clear();
        for (const auto& pair : state.bidBookL3) state.prevBidVolumes_l[pair.first] = pair.second.totalSizeAtLevel_l;
        state.prevAskVolumes_l.clear();
        for (const auto& pair : state.askBookL3) state.prevAskVolumes_l[pair.first] = pair.second.totalSizeAtLevel_l;
      }

      if (dataForDownstream.topBids[0].price_l <= 0 || dataForDownstream.topAsks[0].price_l <= 0) {
        continue;
      }

      L3DataForQueue book_data_for_q;
      book_data_for_q.tp = dataForDownstream.tp;
      book_data_for_q.exchange = dataForDownstream.exchange;
      book_data_for_q.assetPair = dataForDownstream.assetPair;
      book_data_for_q.topBids = dataForDownstream.topBids;
      book_data_for_q.topAsks = dataForDownstream.topAsks;

      for (size_t i = 0; i < dataForDownstream.top_5_bids_levels.size(); ++i) {
        book_data_for_q.topBidsFeatures[i] =
            InstrumentState::calculate_level_features_from_simple(dataForDownstream.top_5_bids_levels[i], dataForDownstream.tp);
      }
      for (size_t i = 0; i < dataForDownstream.top_5_asks_levels.size(); ++i) {
        book_data_for_q.topAsksFeatures[i] =
            InstrumentState::calculate_level_features_from_simple(dataForDownstream.top_5_asks_levels[i], dataForDownstream.tp);
      }
      // And this will work
      this->qdb_queue_.push(std::move(book_data_for_q));

      if (!dataForDownstream.is_snapshot && dataForDownstream.top_5_levels_changed) {
        CalculatedFlowFeatures flow_data_for_q;
        flow_data_for_q.tp = dataForDownstream.tp;
        flow_data_for_q.assetPair = dataForDownstream.assetPair;
        auto bid_ofi_levels = InstrumentState::calculate_ofi_from_simple(dataForDownstream.top_5_bids_levels, dataForDownstream.prev_bid_volumes_l);
        auto ask_ofi_levels = InstrumentState::calculate_ofi_from_simple(dataForDownstream.top_5_asks_levels, dataForDownstream.prev_ask_volumes_l);
        for (int k_ofi = 0; k_ofi < 5; ++k_ofi) {
          *(&flow_data_for_q.ofi_level1 + k_ofi) = bid_ofi_levels[k_ofi] - ask_ofi_levels[k_ofi];
        }
        this->qdb_queue_.push(std::move(flow_data_for_q));
      }
      // In kraken_l3_collector.cpp -> MyEventHandler::instrument_worker_main

      // FIX: Use the fully qualified name for the message type to fix the build error.
    } else if (message.getType() == ccapi::Message::Type::MARKET_DATA_EVENTS_TRADE) {
      // Logic is now entirely inside the loop to process each trade's unique timestamp.
      for (const auto& element : message.getElementList()) {
        const auto& tradeDataMap = element.getNameValueMap();
        if (tradeDataMap.empty()) continue;

        auto getMapValue = [&](const std::string& key) { return tradeDataMap.count(key) ? tradeDataMap.at(key) : ""; };

        // Get the individual timestamp for THIS specific trade.
        std::string timeStr = getMapValue("event_time");
        if (timeStr.empty()) {
          continue;
        }

        std::string priceStr = getMapValue(CCAPI_LAST_PRICE);
        std::string sizeStr = getMapValue(CCAPI_LAST_SIZE);

        if (priceStr.empty() || sizeStr.empty()) {
          continue;
        }
        double price_d = 0.0, size_d = 0.0;
        try {
          price_d = std::stod(priceStr);
          size_d = std::stod(sizeStr);
        } catch (const std::exception&) {
          continue;
        }

        if (price_d <= 0 || size_d <= 0) {
          continue;
        }

        // --- NEW AND SMARTER TIMESTAMP LOGIC ---
        ccapi::TimePoint corrected_tp;
        {
          // 1. Trust the exchange's timestamp first.
          corrected_tp = ccapi::UtilTime::parse(timeStr);

          // 2. Check if this new timestamp is valid (i.e., after the last one).
          //    This condition now intelligently handles both true duplicates and out-of-order messages.
          if (state.last_trade_timestamp.time_since_epoch().count() > 0 && corrected_tp <= state.last_trade_timestamp) {
            // 3. Only if it's not valid, apply the +1 microsecond safety offset.
            corrected_tp = state.last_trade_timestamp + std::chrono::microseconds(1);
          }
          // 4. Always update the state with the new, guaranteed-unique timestamp for the next trade.
          state.last_trade_timestamp = corrected_tp;
        }

        std::string side = getMapValue(CCAPI_EM_ORDER_SIDE);
        std::transform(side.begin(), side.end(), side.begin(), ::toupper);
        std::string ord_type = getMapValue("ORD_TYPE");

        this->qdb_queue_.push(TradeDataForQueue{state.exchange, state.assetPair, price_d, size_d, side, ord_type, corrected_tp});
      }
    }
  }
  std::cout << "[WORKER] Thread shutting down for " << instrumentKey << std::endl;
}
}  // namespace kraken_l3_collector_questdb

static std::string getEnvVar(const char* name) {
  const char* val = std::getenv(name);
  if (!val || !*val) {
    std::cerr << "ERROR: Environment variable " << name << " is not set or empty\n";
    std::exit(EXIT_FAILURE);
  }
  return std::string(val);
}

int main(int argc, char** argv) {
  using namespace std::chrono_literals;

  std::string key = getEnvVar("CCAPI_KRAKEN_API_KEY");
  std::string secret = getEnvVar("CCAPI_KRAKEN_API_SECRET");
  std::map<std::string, std::string> credentials{{CCAPI_KRAKEN_API_KEY, key}, {CCAPI_KRAKEN_API_SECRET, secret}};
  ccapi::SessionOptions sessionOptions;
  ccapi::SessionConfigs sessionConfigs;
  sessionConfigs.setCredential(credentials);

  std::unique_ptr<questdb::ingress::line_sender> qdbSender;
  try {
    const char* hostEnv = std::getenv("QUESTDB_HOST");
    std::string host = hostEnv ? hostEnv : kraken_l3_collector_questdb::QUESTDB_HOST;
    const char* portEnv = std::getenv("QUESTDB_ILP_TCP_PORT");
    int port = portEnv ? std::stoi(portEnv) : kraken_l3_collector_questdb::QUESTDB_ILP_TCP_PORT;
    std::string conf = "tcp::addr=" + host + ":" + std::to_string(port) + ";";
    qdbSender =
        std::make_unique<questdb::ingress::line_sender>(questdb::ingress::line_sender::from_conf(questdb::ingress::utf8_view{conf.data(), conf.size()}));
    std::cout << "INFO KRAKEN: QuestDB sender initialized." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "CRITICAL ERROR: Failed to init QuestDB sender: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  std::promise<std::string> tokenPromise;
  auto tokenFuture = tokenPromise.get_future();
  const std::string tokenCorId = "KRAKEN_WS_TOKEN";
  kraken_l3_collector_questdb::MyEventHandler eventHandler(std::move(qdbSender), tokenPromise, tokenCorId);

  // 2. Create session
  ccapi::EventDispatcher dispatcher(1);
  ccapi::Session session(sessionOptions, sessionConfigs, &eventHandler, &dispatcher);

  // 3. NOW set the session pointer in the handler
  eventHandler.setSession(&session);

  const std::vector<std::string> PAIRS_TO_SUBSCRIBE = {"BTC/USD",  "ETH/USD", "USDT/USD", "SOL/USD",  "XRP/USD",
                                                       "DOGE/USD", "ADA/USD", "LTC/USD",  "LINK/USD", "DOT/USD"};
  // const std::vector<std::string> PAIRS_TO_SUBSCRIBE = {"USDT/USD"};

  std::vector<ccapi::Subscription> emSubscriptions;
  for (const auto& pair_str : PAIRS_TO_SUBSCRIBE) {
    emSubscriptions.emplace_back(CCAPI_EXCHANGE_NAME_KRAKEN, pair_str, CCAPI_EM_PRIVATE_TRADE, "", "TRADE_" + pair_str, std::map<std::string, std::string>{});
  }
  std::cout << "INFO KRAKEN: Subscribing to execution management..." << std::endl;
  session.subscribe(emSubscriptions);

  std::cout << "INFO KRAKEN: Waiting for WS token (15s)..." << std::endl;
  std::string wsToken;
  if (tokenFuture.wait_for(15s) == std::future_status::ready) {
    wsToken = tokenFuture.get();
    if (wsToken.empty()) {
      std::cerr << "CRITICAL: Received empty WS token. Check credentials or logs." << std::endl;
      session.stop();
      dispatcher.stop();
      return EXIT_FAILURE;
    }
    std::cout << "INFO KRAKEN: Obtained WS token." << std::endl;
    eventHandler.setWsToken(wsToken);
  } else {
    std::cerr << "CRITICAL: Timed out waiting for WS token." << std::endl;
    session.stop();
    dispatcher.stop();
    return EXIT_FAILURE;
  }

  std::cout << "INFO KRAKEN: Subscribing to Market Data (L3 & Public Trades) with token..." << std::endl;
  std::vector<ccapi::Subscription> mdL3Subscriptions;
  std::vector<ccapi::Subscription> mdTradeSubscriptions;

  for (const auto& canonicalPair : PAIRS_TO_SUBSCRIBE) {
    std::string symbol = canonicalPair;
    size_t depth = 10;  // The depth we are subscribing to

    // L3 Subscription (goes to the authenticated endpoint)
    ccapi::Subscription l3Sub("market_data_l3",  // Custom service for ws-auth
                              CCAPI_EXCHANGE_NAME_KRAKEN, symbol, ccapi::CCAPI_KRAKEN_FIELD_LEVEL3, "depth=" + std::to_string(depth), "L3_" + canonicalPair,
                              {{"token", wsToken}});
    mdL3Subscriptions.push_back(l3Sub);
    eventHandler.addSubscriptionDetails("L3_" + canonicalPair, l3Sub);

    // Public Trade Subscription (goes to the public endpoint)
    ccapi::Subscription publicTradeSub(CCAPI_EXCHANGE_NAME_KRAKEN,                                   // Default service for ws
                                       symbol, CCAPI_TRADE, "", "PUBLIC_TRADE_" + canonicalPair, {}  // No token needed
    );
    mdTradeSubscriptions.push_back(publicTradeSub);
    eventHandler.addSubscriptionDetails("PUBLIC_TRADE_" + canonicalPair, publicTradeSub);
  }

  if (!mdL3Subscriptions.empty()) {
    session.subscribe(mdL3Subscriptions);
  }
  if (!mdTradeSubscriptions.empty()) {
    session.subscribe(mdTradeSubscriptions);
  }
  // std::cout << "INFO KRAKEN: Starting CCAPI session and dispatcher..." << std::endl;
  // dispatcher.start();

  int runSeconds = 60;
  if (argc > 1) {
    try {
      runSeconds = std::stoi(argv[1]);
    } catch (const std::exception&) {
    }
  }
  std::cout << "INFO KRAKEN: Running collector for " << runSeconds << "s..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(runSeconds));
  std::cout << "INFO KRAKEN: Stopping session..." << std::endl;
  session.stop();
  dispatcher.stop();
  std::cout << "INFO KRAKEN: Collector finished." << std::endl;
  return EXIT_SUCCESS;
}
