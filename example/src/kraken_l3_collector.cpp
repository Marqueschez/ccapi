// kraken_hourly_collector_L3.cpp

// MODIFICATION: Include the specific service headers FIRST.
#include "ccapi_cpp/service/ccapi_execution_management_service_kraken.h"
#include "ccapi_cpp/service/ccapi_market_data_service_kraken.h"
#include "json_utils.h"  // ← for safeGetString()

// Now, include the rest of the CCAPI headers
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

  L3Order(std::string id = "", int64_t sz_l = 0, ccapi::TimePoint tp = {}) : orderId(std::move(id)), size_l(sz_l), arrivalTime(tp) {}
};

struct L3PriceLevel {
  int64_t price_l;
  std::string price;  // The original, clean price string
  std::map<std::string, L3Order> orders;
  int64_t totalSizeAtLevel_l{0};

  L3PriceLevel(int64_t p_l = 0, std::string p_s = "") : price_l(p_l), price(std::move(p_s)) {}

  // applyChange now only needs the integer size
  void applyChange(const std::string& orderId, int64_t newSize_l, const ccapi::TimePoint& messageTime) {
    auto it = orders.find(orderId);
    if (it != orders.end()) {
      totalSizeAtLevel_l -= it->second.size_l;
      if (newSize_l <= 0) {
        orders.erase(it);
      } else {
        it->second.size_l = newSize_l;
        it->second.arrivalTime = messageTime;
        totalSizeAtLevel_l += newSize_l;
      }
    } else {
      if (newSize_l > 0) {
        orders.emplace(orderId, L3Order{orderId, newSize_l, messageTime});
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
  int64_t price_l;  // Use integers
  int64_t size_l;   // Use integers
  int64_t orders_count;

  void clear() {
    price_l = 0;
    size_l = 0;
    orders_count = 0;
  }
};

struct InstrumentState {
  std::map<std::string, std::pair<int64_t, std::string>> orderIdToSideAndPriceKey;
  std::string exchange;
  std::string assetPair;
  std::map<int64_t, L3PriceLevel, std::greater<int64_t>> bidBookL3;
  std::map<int64_t, L3PriceLevel> askBookL3;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topBidsForQuest;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topAsksForQuest;
  bool bookSnapshotReceived{false};
  std::map<int64_t, std::string> prevBidVolumes;
  std::map<int64_t, std::string> prevAskVolumes;
  std::mutex instrumentMutex;
  std::vector<ccapi::Message> pendingUpdates;
  ccapi::TimePoint last_trade_timestamp;

  InstrumentState(std::string ex = "", std::string ap = "") : exchange(std::move(ex)), assetPair(std::move(ap)) {}

  void clearL3Book() {
    bidBookL3.clear();
    askBookL3.clear();
    orderIdToSideAndPriceKey.clear();

    for (auto& l : topBidsForQuest) l.clear();
    for (auto& l : topAsksForQuest) l.clear();
    bookSnapshotReceived = false;
  }

  void updateTopNForQuestDB() {
    for (auto& l : topBidsForQuest) l.clear();
    for (auto& l : topAsksForQuest) l.clear();
    int i = 0;
    for (const auto& pair_ : bidBookL3) {
      if (i >= NUM_AGGREGATED_LEVELS_TO_SEND) break;
      topBidsForQuest[i].price_l = pair_.second.price_l;
      topBidsForQuest[i].size_l = pair_.second.totalSizeAtLevel_l;  // Copy the raw integer
      topBidsForQuest[i].orders_count = pair_.second.getNumOrdersAtLevel();
      i++;
    }
    i = 0;
    for (const auto& pair_ : askBookL3) {
      if (i >= NUM_AGGREGATED_LEVELS_TO_SEND) break;
      topAsksForQuest[i].price_l = pair_.second.price_l;
      topAsksForQuest[i].size_l = pair_.second.totalSizeAtLevel_l;  // Copy the raw integer
      topAsksForQuest[i].orders_count = pair_.second.getNumOrdersAtLevel();
      i++;
    }
  }

  void applyL3Update(const std::string& orderId, const std::string& priceStr, const std::string& sizeStr, const std::string& sideStr,
                     const ccapi::TimePoint& messageTime) {
    auto it = orderIdToSideAndPriceKey.find(orderId);
    bool orderExists = (it != orderIdToSideAndPriceKey.end());
    int64_t size_l = sizeStrToLong(sizeStr);

    // Case 1: Order is being deleted (or size reduced to zero)
    if (size_l <= 0) {
      if (orderExists) {
        int64_t old_price_l = it->second.first;
        bool wasBid = (it->second.second == "bid");

        if (wasBid) {
          auto& level = bidBookL3.at(old_price_l);
          level.applyChange(orderId, 0, messageTime);
          if (level.getNumOrdersAtLevel() == 0) {
            bidBookL3.erase(old_price_l);
          }
        } else {  // was Ask
          auto& level = askBookL3.at(old_price_l);
          level.applyChange(orderId, 0, messageTime);
          if (level.getNumOrdersAtLevel() == 0) {
            askBookL3.erase(old_price_l);
          }
        }
        orderIdToSideAndPriceKey.erase(it);
      }
      return;  // Done with this update.
    }

    // If we reach here, size_l > 0, so it's an Add or Modify.

    // Case 2: Order already exists, so it's a modification.
    if (orderExists) {
      int64_t old_price_l = it->second.first;
      bool wasBid = (it->second.second == "bid");
      int64_t new_price_l = priceStrToLong(priceStr);

      // Sub-case 2a: Price has NOT changed, only size is modified.
      if (new_price_l == old_price_l) {
        if (wasBid) {
          bidBookL3.at(old_price_l).applyChange(orderId, size_l, messageTime);
        } else {
          askBookL3.at(old_price_l).applyChange(orderId, size_l, messageTime);
        }
      }
      // Sub-case 2b: Price HAS changed. This is a "move".
      else {
        // Step 1: Delete the order from its OLD price level.
        if (wasBid) {
          auto& level = bidBookL3.at(old_price_l);
          level.applyChange(orderId, 0, messageTime);
          if (level.getNumOrdersAtLevel() == 0) {
            bidBookL3.erase(old_price_l);
          }
        } else {
          auto& level = askBookL3.at(old_price_l);
          level.applyChange(orderId, 0, messageTime);
          if (level.getNumOrdersAtLevel() == 0) {
            askBookL3.erase(old_price_l);
          }
        }

        // Step 2: Add the order to its NEW price level on the SAME side.
        if (wasBid) {
          auto& level = bidBookL3[new_price_l];
          if (level.price.empty()) {  // First order at this new level
            level.price_l = new_price_l;
            level.price = priceStr;
          }
          level.applyChange(orderId, size_l, messageTime);
        } else {  // was Ask
          auto& level = askBookL3[new_price_l];
          if (level.price.empty()) {  // First order at this new level
            level.price_l = new_price_l;
            level.price = priceStr;
          }
          level.applyChange(orderId, size_l, messageTime);
        }

        // Step 3: Update the index to point to the new price.
        it->second.first = new_price_l;
      }
    }
    // Case 3: Order does NOT exist, so it's a genuinely new order.
    else {
      if (priceStr.empty() || sideStr.empty()) return;

      bool isBid = (sideStr == "bid" || sideStr == "buy");
      int64_t price_l = priceStrToLong(priceStr);

      if (isBid) {
        auto& level = bidBookL3[price_l];
        if (level.price.empty()) {  // First order at this new level
          level.price_l = price_l;
          level.price = priceStr;
        }
        level.applyChange(orderId, size_l, messageTime);
      } else {  // is Ask
        auto& level = askBookL3[price_l];
        if (level.price.empty()) {  // First order at this new level
          level.price_l = price_l;
          level.price = priceStr;
        }
        level.applyChange(orderId, size_l, messageTime);
      }

      // Add the new order to our index.
      orderIdToSideAndPriceKey[orderId] = {price_l, isBid ? "bid" : "ask"};
    }
  }
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
  ccapi::TimePoint tp;
};

struct BookDataForProcessing {
  ccapi::TimePoint tp;
  std::string assetPair;
  std::string exchange;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topBids;
  std::array<AggregatedLevelData, NUM_AGGREGATED_LEVELS_TO_SEND> topAsks;
  // We need the full book state to calculate features later
  std::map<int64_t, L3PriceLevel, std::greater<int64_t>> top_5_bids;
  std::map<int64_t, L3PriceLevel> top_5_asks;
  // We need the previous volumes to calculate OFI
  std::map<int64_t, std::string> prev_top_5_bid_volumes;
  std::map<int64_t, std::string> prev_top_5_ask_volumes;
  bool is_snapshot{false};
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

std::pair<L3Order, L3Order> findQueueEnds(const L3PriceLevel& level) {
  if (level.orders.empty()) {
    return {{}, {}};
  }
  auto it = level.orders.begin();
  L3Order frontOrder = it->second;
  L3Order backOrder = it->second;
  for (++it; it != level.orders.end(); ++it) {
    if (it->second.arrivalTime < frontOrder.arrivalTime) {
      frontOrder = it->second;
    }
    if (it->second.arrivalTime > backOrder.arrivalTime) {
      backOrder = it->second;
    }
  }
  return {frontOrder, backOrder};
}

CalculatedLevelFeatures calculate_level_features(const L3PriceLevel& level, const ccapi::TimePoint& currentTime) {
  CalculatedLevelFeatures features;
  if (level.orders.empty()) {
    return features;
  }
  auto queueEnds = findQueueEnds(level);
  L3Order& topOrder = queueEnds.first;
  features.topOrderSize = longToSize(topOrder.size_l);  // Use the double directly
  features.topOrderAge_ms = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - topOrder.arrivalTime).count();

  std::vector<double> orderSizes;
  orderSizes.reserve(level.orders.size());
  for (const auto& pair : level.orders) {
    orderSizes.push_back(longToSize(pair.second.size_l));  // Use the double directly
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

template <typename BookType>
std::array<double, 5> calculate_ofi(const BookType& currentBook, const std::map<int64_t, std::string>& prevVolumes) {
  std::array<double, 5> ofi_levels{};
  auto current_it = currentBook.begin();
  for (int i = 0; i < 5 && current_it != currentBook.end(); ++i, ++current_it) {
    int64_t price_l = current_it->first;
    // Use the double member directly
    double current_volume = longToSize(current_it->second.totalSizeAtLevel_l);
    double prev_volume = 0.0;

    auto prev_it = prevVolumes.find(price_l);
    if (prev_it != prevVolumes.end()) {
      prev_volume = std::stod(prev_it->second);
    }
    ofi_levels[i] = current_volume - prev_volume;
  }
  return ofi_levels;
}

class MyEventHandler : public ccapi::EventHandler {
 public:
  MyEventHandler(std::unique_ptr<questdb::ingress::line_sender> sender, std::promise<std::string>& token_promise, const std::string& token_request_cor_id)
      : sender_(std::move(sender)), tokenPromiseRef_(token_promise), tokenRequestCorId_(token_request_cor_id) {
    qdb_writer_thread_ = std::thread(&MyEventHandler::qdb_writer_main, this);
  }

  ~MyEventHandler() override {
    qdb_queue_.shutdown();
    if (qdb_writer_thread_.joinable()) {
      qdb_writer_thread_.join();
    }
    std::cout << "Event handler destroyed." << std::endl;
  }

  void addSubscriptionDetails(const std::string& corId, const std::string& exchange_name, const std::string& instrument) {
    std::lock_guard<std::mutex> lock(mapMutex_);
    std::string instrumentKey = exchange_name + "_" + instrument;
    correlationToInstrumentKey_[corId] = instrumentKey;
    if (instrumentStates_.find(instrumentKey) == instrumentStates_.end()) {
      // This is the most explicit way to construct an object in-place in a map.
      // It tells the compiler:
      // 1. Use piecewise construction.
      // 2. The first tuple of arguments is for the key's constructor.
      // 3. The second tuple of arguments is for the value's constructor.
      instrumentStates_.emplace(std::piecewise_construct, std::forward_as_tuple(instrumentKey), std::forward_as_tuple(exchange_name, instrument));
    }
  }

  // This is the clean, correct version of your snapshot handler.
  void processKrakenL3Snapshot(InstrumentState& state, const ccapi::Message& message) {
    // Step 1: Clear the old book state completely.
    state.clearL3Book();
    const auto& messageTime = message.getTime();

    // Step 2: Process every order in the snapshot message.
    for (const auto& element : message.getElementList()) {
      const auto& dataMap = element.getNameValueMap();
      std::string orderId = safeGetString(dataMap, CCAPI_EM_ORDER_ID);
      std::string priceStr = safeGetString(dataMap, CCAPI_EM_ORDER_LIMIT_PRICE);
      std::string sizeStr = safeGetString(dataMap, CCAPI_EM_ORDER_QUANTITY);
      std::string sideStr = safeGetString(dataMap, CCAPI_EM_ORDER_SIDE);

      if (orderId.empty() || priceStr.empty()) continue;

      // By definition, every order in a snapshot is a new addition.
      // We delegate this directly to our central state manager.
      state.applyL3Update(orderId, priceStr, sizeStr, sideStr, messageTime);
    }
    state.bookSnapshotReceived = true;
    std::cout << "[INFO] Snapshot for " << state.assetPair << " processed. Now applying " << state.pendingUpdates.size() << " pending updates." << std::endl;

    // Step 3: Process any updates that were queued while we were handling the snapshot.
    for (const auto& pendingMsg : state.pendingUpdates) {
      // We need to loop through the elements of the pending message, too.
      for (const auto& element : pendingMsg.getElementList()) {
        const auto& dataMap = element.getNameValueMap();
        auto getMapValue = [&](const std::string& key) { return dataMap.count(key) ? dataMap.at(key) : ""; };

        std::string eventType = getMapValue("event");
        std::string orderId = getMapValue(CCAPI_EM_ORDER_ID);
        std::string priceStr = getMapValue(CCAPI_EM_ORDER_LIMIT_PRICE);
        std::string sizeStr = getMapValue(CCAPI_EM_ORDER_QUANTITY);
        std::string sideStr = getMapValue(CCAPI_EM_ORDER_SIDE);

        if (orderId.empty()) continue;

        // Use the same delete/modify logic from the main update handler
        if (eventType == "delete") {
          state.applyL3Update(orderId, "", "0", sideStr, pendingMsg.getTime());
        } else {
          state.applyL3Update(orderId, priceStr, sizeStr, sideStr, pendingMsg.getTime());
        }
      }
    }
    state.pendingUpdates.clear();

    // Step 4: After ALL changes are made, update the top-N levels for QuestDB ONCE.
    state.updateTopNForQuestDB();
  }

  bool processKrakenL3Update(InstrumentState& state, const ccapi::Message& message) {
    const auto& messageTime = message.getTime();

    for (const auto& element : message.getElementList()) {
      const auto& dataMap = element.getNameValueMap();
      auto getMapValue = [&](const std::string& key) { return dataMap.count(key) ? dataMap.at(key) : ""; };

      std::string eventType = getMapValue("event");
      std::string orderId = getMapValue(CCAPI_EM_ORDER_ID);
      std::string priceStr = getMapValue(CCAPI_EM_ORDER_LIMIT_PRICE);
      std::string sizeStr = getMapValue(CCAPI_EM_ORDER_QUANTITY);
      std::string sideStr = getMapValue(CCAPI_EM_ORDER_SIDE);

      // --- THIS IS THE CRITICAL DIAGNOSTIC LOG ---
      // It will now ONLY print for live updates, not the noisy snapshot.
      {
        std::lock_guard<std::mutex> lock(logMutex_);
        if (logCounter_ > 0) {
          std::cout << "[L3-DEBUG] asset=" << state.assetPair << ", event=" << eventType << ", id=" << orderId << ", price=" << priceStr << ", size=" << sizeStr
                    << std::endl;
          logCounter_--;
        }
      }

      if (orderId.empty()) continue;

      // This is the correct logic you already developed.
      if (eventType == "delete") {
        state.applyL3Update(orderId, "", "0", sideStr, messageTime);
      } else {
        state.applyL3Update(orderId, priceStr, sizeStr, sideStr, messageTime);
      }
    }

    // Update the top-N levels once per message.
    state.updateTopNForQuestDB();
    return true;
  }

  bool processEvent(const ccapi::Event& event, ccapi::Session* session) override {
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
    } else if (event.getType() == ccapi::Event::Type::SUBSCRIPTION_DATA) {
      for (const auto& message : event.getMessageList()) {
        if (message.getCorrelationIdList().empty()) continue;
        std::string corId = message.getCorrelationIdList().at(0);
        std::string instrumentKey;
        {
          std::lock_guard<std::mutex> lookupLock(mapMutex_);  // Protects the correlation map
          auto it = correlationToInstrumentKey_.find(corId);
          if (it == correlationToInstrumentKey_.end()) continue;
          instrumentKey = it->second;
        }

        if (message.getType() == ccapi::Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH) {
          if (corId.rfind("L3_", 0) == 0) {
            InstrumentState& state = instrumentStates_.at(instrumentKey);
            BookDataForProcessing data_to_process;  // Create our temporary container
            {
              std::lock_guard<std::mutex> lock(state.instrumentMutex);

              // 1. Apply the update to the live book
              data_to_process.is_snapshot = (message.getRecapType() == ccapi::Message::RecapType::SOLICITED);
              if (data_to_process.is_snapshot) {
                processKrakenL3Snapshot(state, message);
              } else {
                if (state.bookSnapshotReceived) {
                  processKrakenL3Update(state, message);
                } else {
                  state.pendingUpdates.push_back(message);
                  return true;  // Exit early, nothing more to do
                }
              }

              // 2. Update the timestamp
              auto& last_book_tp = last_book_timestamp_by_instrument_[state.assetPair];
              data_to_process.tp = message.getTime();
              if (last_book_tp.time_since_epoch().count() > 0 && data_to_process.tp <= last_book_tp) {
                data_to_process.tp = last_book_tp + std::chrono::nanoseconds(1);
              }
              last_book_tp = data_to_process.tp;

              // 3. Copy out the MINIMAL data needed for later processing
              // This is a deep copy, which is expensive, but it allows us to release the lock.
              data_to_process.assetPair = state.assetPair;
              data_to_process.exchange = state.exchange;

              state.updateTopNForQuestDB();  // This is fast, just copies scalars

              data_to_process.topBids = state.topBidsForQuest;
              data_to_process.topAsks = state.topAsksForQuest;

              int k = 0;
              for (const auto& pair_ : state.bidBookL3) {
                if (k++ >= 5) break;
                data_to_process.top_5_bids[pair_.first] = pair_.second;
              }
              k = 0;
              for (const auto& pair_ : state.askBookL3) {
                if (k++ >= 5) break;
                data_to_process.top_5_asks[pair_.first] = pair_.second;
              }

              // Copy only the top 5 previous volumes for OFI calculation
              k = 0;
              for (auto const& [price_l, vol_str] : state.prevBidVolumes) {
                if (k++ >= 5) break;
                data_to_process.prev_top_5_bid_volumes[price_l] = vol_str;
              }
              k = 0;
              for (auto const& [price_l, vol_str] : state.prevAskVolumes) {
                if (k++ >= 5) break;
                data_to_process.prev_top_5_ask_volumes[price_l] = vol_str;
              }

              // 4. Update the live "previous volumes" for the NEXT event
              state.prevBidVolumes.clear();
              for (const auto& pair : state.bidBookL3) state.prevBidVolumes[pair.first] = pair.second.getTotalSizeAtLevelStr();
              state.prevAskVolumes.clear();
              for (const auto& pair : state.askBookL3) state.prevAskVolumes[pair.first] = pair.second.getTotalSizeAtLevelStr();

            }  // LOCK IS RELEASED HERE
            // Now, all expensive work happens outside the lock, on our private copy of the data.

            // B. Check for completeness
            auto isLevelComplete = [](const auto& lvl) { return lvl.price_l > 0 && lvl.size_l > 0 && lvl.orders_count > 0; };
            bool fullBids = std::all_of(data_to_process.topBids.begin(), data_to_process.topBids.end(), isLevelComplete);
            bool fullAsks = std::all_of(data_to_process.topAsks.begin(), data_to_process.topAsks.end(), isLevelComplete);

            if (!fullBids || !fullAsks) {
              std::lock_guard<std::mutex> lock(logMutex_);
              std::cout << "[INFO] DROPPED L3 BOOK for " << data_to_process.assetPair << " (incomplete top-N)" << std::endl;
              return true;
            }

            // C. Create the final objects to be queued
            L3DataForQueue book_data_for_q;
            book_data_for_q.tp = data_to_process.tp;
            book_data_for_q.exchange = data_to_process.exchange;
            book_data_for_q.assetPair = data_to_process.assetPair;
            book_data_for_q.topBids = data_to_process.topBids;
            book_data_for_q.topAsks = data_to_process.topAsks;

            // D. Calculate features on our private book copy
            int j = 0;
            for (const auto& pair_ : data_to_process.top_5_bids) {
              if (j >= 5) break;
              book_data_for_q.topBidsFeatures[j++] = calculate_level_features(pair_.second, data_to_process.tp);
            }
            j = 0;
            for (const auto& pair_ : data_to_process.top_5_asks) {
              if (j >= 5) break;
              book_data_for_q.topAsksFeatures[j++] = calculate_level_features(pair_.second, data_to_process.tp);
            }

            // E. Push to the queue
            qdb_queue_.push(book_data_for_q);

            // F. Calculate OFI and push if needed
            if (!data_to_process.is_snapshot) {
              CalculatedFlowFeatures flow_data_for_q;
              flow_data_for_q.tp = data_to_process.tp;
              flow_data_for_q.assetPair = data_to_process.assetPair;

              auto bid_ofi_levels = calculate_ofi(data_to_process.top_5_bids, data_to_process.prev_top_5_bid_volumes);
              auto ask_ofi_levels = calculate_ofi(data_to_process.top_5_asks, data_to_process.prev_top_5_ask_volumes);
              for (int k_ofi = 0; k_ofi < 5; ++k_ofi) {
                *(&flow_data_for_q.ofi_level1 + k_ofi) = bid_ofi_levels[k_ofi] - ask_ofi_levels[k_ofi];
              }
              qdb_queue_.push(std::move(flow_data_for_q));
            }
          }
        } else if (message.getType() == ccapi::Message::Type::MARKET_DATA_EVENTS_TRADE) {
          std::string instrumentKey;
          {
            std::lock_guard<std::mutex> lock(mapMutex_);
            auto it = correlationToInstrumentKey_.find(corId);
            if (it == correlationToInstrumentKey_.end()) continue;
            instrumentKey = it->second;
          }

          InstrumentState& state = instrumentStates_.at(instrumentKey);
          ccapi::TimePoint base_tp = message.getTime();
          int trade_index_in_batch = 0;

          for (const auto& element : message.getElementList()) {
            const auto& tradeDataMap = element.getNameValueMap();
            if (tradeDataMap.empty()) continue;

            // Extract and validate *before* the lock
            auto getMapValue = [&](const std::string& key) { return tradeDataMap.count(key) ? tradeDataMap.at(key) : ""; };
            std::string priceStr = getMapValue(CCAPI_LAST_PRICE);
            std::string sizeStr = getMapValue(CCAPI_LAST_SIZE);

            if (priceStr.empty() || sizeStr.empty()) {
              std::lock_guard<std::mutex> lock(logMutex_);
              std::cout << "[INFO] DROPPED TRADE for " << state.assetPair << " (reason: empty price/size string)" << std::endl;
              continue;
            }

            // BUGFIX #2b: Perform the conversion to double ONCE here.
            double price_d = 0.0, size_d = 0.0;
            try {
              price_d = std::stod(priceStr);
              size_d = std::stod(sizeStr);
            } catch (const std::exception&) {
              std::lock_guard<std::mutex> lock(logMutex_);
              std::cout << "[INFO] DROPPED TRADE for " << state.assetPair << " (reason: std::stod conversion failed)" << std::endl;
              continue;  // Invalid number format
            }

            // This check is now on the double values.
            if (price_d <= 0 || size_d <= 0) {
              std::lock_guard<std::mutex> lock(logMutex_);
              std::cout << "[INFO] DROPPED TRADE for " << state.assetPair << " (reason: zero or negative price/size)" << std::endl;
              continue;
            }

            ccapi::TimePoint corrected_tp;
            {  // Lock only for the critical section: timestamp adjustment
              std::lock_guard<std::mutex> lock(state.instrumentMutex);
              corrected_tp = base_tp + std::chrono::microseconds(trade_index_in_batch++);
              if (state.last_trade_timestamp.time_since_epoch().count() > 0 && corrected_tp <= state.last_trade_timestamp) {
                corrected_tp = state.last_trade_timestamp + std::chrono::microseconds(1);
              }
              state.last_trade_timestamp = corrected_tp;
            }

            // Push to the queue. We believe the data is good here.
            std::string side = getMapValue(CCAPI_EM_ORDER_SIDE);
            std::transform(side.begin(), side.end(), side.begin(), ::toupper);
            qdb_queue_.push(TradeDataForQueue{state.exchange, state.assetPair, price_d, size_d, side, corrected_tp});
          }
        }
      }
    } else if (event.getType() == ccapi::Event::Type::SUBSCRIPTION_STATUS) {
      for (const auto& msg : event.getMessageList()) {
        std::cout << "KRAKEN Subscription Status (CorID: " << (msg.getCorrelationIdList().empty() ? "N/A" : msg.getCorrelationIdList().at(0))
                  << "): " << msg.toStringPretty(2, 0) << std::endl;
      }
    }
    return true;
  }

 private:
  void qdb_writer_main() {
    questdb::ingress::line_sender_buffer send_buffer;
    auto last_flush = std::chrono::steady_clock::now();
    auto last_log = std::chrono::steady_clock::now();
    QdbEvent event_data;

    // The main loop now has two parts: a blocking wait, and a greedy drain.
    while (true) {
      // Step 1: Block and wait for the FIRST event to arrive.
      // If the queue is shut down, this will return false and we exit.
      if (!qdb_queue_.wait_and_pop(event_data)) {
        break;  // Shutdown signal received
      }

      // We have at least one event, so process it.
      process_qdb_event(event_data, send_buffer);

      // Step 2: GREEDY DRAIN. Try to empty the rest of the queue without waiting.
      // This packs the buffer as full as possible.
      while (qdb_queue_.try_pop(event_data)) {  // try_pop is non-blocking
        process_qdb_event(event_data, send_buffer);
      }

      // Step 3: Check flush conditions *after* the greedy drain.
      auto now = std::chrono::steady_clock::now();
      if (send_buffer.size() > 256 * 1024 || (send_buffer.size() > 0 && std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count() >= 2)) {
        try {
          try {
            sender_->flush(send_buffer);
          } catch (const std::exception& e) {
            std::cerr << "QuestDB writer: flush failed, attempting reconnect... (" << e.what() << ")" << std::endl;
            try {
              // No reconnect() method; just retry flush
              sender_->flush(send_buffer);  // Retry flush
              std::cerr << "QuestDB writer: retry flush successful." << std::endl;
            } catch (const std::exception& e2) {
              std::cerr << "QuestDB writer: retry flush failed: " << e2.what() << std::endl;
            }
          }
          send_buffer.clear();
        } catch (const std::exception& e) {
          std::cerr << "QDB WRITER FLUSH/RECONNECT ERROR: " << e.what() << std::endl;
          send_buffer.clear();  // Clear buffer on error to prevent resending bad data
        }
        last_flush = now;
      }

      if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 5) {
        std::cout << "[MONITOR] QDB writer queue size: " << qdb_queue_.size() << std::endl;
        last_log = now;
      }
    }
    // Final flush on shutdown
    if (send_buffer.size() > 0) {
      try {
        sender_->flush(send_buffer);
      } catch (const std::exception& e) {
        std::cerr << "QuestDB FINAL FLUSH ERROR: " << e.what() << std::endl;
      }
    }

    try {
      sender_->close();
    } catch (const std::exception& e) {
      std::cerr << "QuestDB SENDER CLOSE ERROR: " << e.what() << std::endl;
    }
  }

  // You will also need to add try_pop to your ThreadSafeQueue and add a helper
  // function to avoid duplicating the giant std::visit block.

  // --- ADD THIS HELPER FUNCTION inside the MyEventHandler class ---
  void process_qdb_event(QdbEvent& event_data, questdb::ingress::line_sender_buffer& send_buffer) {
    try {
      std::visit(
          [&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;

            // This logic for preventing table-mixing in a single buffer is fine.
            const char* target_table_name = nullptr;
            if constexpr (std::is_same_v<T, L3DataForQueue>)
              target_table_name = "kraken_l3_book_levels_agg";
            else if constexpr (std::is_same_v<T, TradeDataForQueue>)
              target_table_name = "kraken_trades";
            else if constexpr (std::is_same_v<T, CalculatedFlowFeatures>)
              target_table_name = "kraken_l3_flow_features";
            if (!target_table_name) return;

            static std::string current_table_in_batch;
            if (!current_table_in_batch.empty() && current_table_in_batch != target_table_name) {
              sender_->flush(send_buffer);
              send_buffer.clear();
            }
            current_table_in_batch = target_table_name;

            std::string safeSymbol = arg.assetPair;
            std::replace(safeSymbol.begin(), safeSymbol.end(), '/', '_');
            auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(arg.tp.time_since_epoch()).count();

            if constexpr (std::is_same_v<T, L3DataForQueue>) {
              if (arg.topBids[NUM_AGGREGATED_LEVELS_TO_SEND - 1].price_l > 0 && arg.topAsks[NUM_AGGREGATED_LEVELS_TO_SEND - 1].price_l > 0) {
                // Now that we know the data is valid, we perform all buffer operations.
                send_buffer.table("kraken_l3_book_levels_agg"_tn);
                send_buffer.symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol});

                // Write Top-N level data
                for (int i = 0; i < NUM_AGGREGATED_LEVELS_TO_SEND; ++i) {
                  std::string level_str = std::to_string(i + 1);
                  if (arg.topBids[i].price_l > 0) {
                    send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_price"}, longToPrice(arg.topBids[i].price_l));
                    send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_size"}, longToSize(arg.topBids[i].size_l));
                    send_buffer.column(questdb::ingress::column_name_view{"bid" + level_str + "_orders"}, arg.topBids[i].orders_count);
                  }
                  if (arg.topAsks[i].price_l > 0) {
                    send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_price"}, longToPrice(arg.topAsks[i].price_l));
                    send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_size"}, longToSize(arg.topAsks[i].size_l));
                    send_buffer.column(questdb::ingress::column_name_view{"ask" + level_str + "_orders"}, arg.topAsks[i].orders_count);
                  }
                }

                // Write L3 feature data
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

                // The commit call is now safely inside the guard.
                send_buffer.at(questdb::ingress::timestamp_nanos{nanos});
              }
              // If the best bid price is 0, we do nothing and discard the event.

            } else if constexpr (std::is_same_v<T, TradeDataForQueue>) {
              // BUGFIX #2d: The final check is now on the double members.
              if (arg.price_d > 0 && arg.size_d > 0) {
                send_buffer.table("kraken_trades"_tn);
                send_buffer.symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol});
                send_buffer.column("price"_cn, arg.price_d);  // Use the double directly
                send_buffer.column("size"_cn, arg.size_d);    // Use the double directly
                send_buffer.column("side"_cn, questdb::ingress::utf8_view{arg.side});
                send_buffer.at(questdb::ingress::timestamp_nanos{nanos});
              } else {
                std::lock_guard<std::mutex> lock(logMutex_);
                std::cout << "[WARN] DROPPED TRADE IN WRITER for " << arg.assetPair << " (reason: invalid data reached queue)" << std::endl;
              }

            } else if constexpr (std::is_same_v<T, CalculatedFlowFeatures>) {
              // This block is also fine. A row of zero OFI is not harmful.
              send_buffer.table("kraken_l3_flow_features"_tn);
              send_buffer.symbol("symbol"_cn, questdb::ingress::utf8_view{safeSymbol});
              send_buffer.column("ofi_level1"_cn, arg.ofi_level1);
              send_buffer.column("ofi_level2"_cn, arg.ofi_level2);
              send_buffer.column("ofi_level3"_cn, arg.ofi_level3);
              send_buffer.column("ofi_level4"_cn, arg.ofi_level4);
              send_buffer.column("ofi_level5"_cn, arg.ofi_level5);
              send_buffer.at(questdb::ingress::timestamp_nanos{nanos});
            }
          },
          event_data);
    } catch (const std::exception& e) {
      std::cerr << "QDB WRITER ERROR: Exception while processing data: " << e.what() << std::endl;
    }
  }

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
  std::map<std::string, ccapi::TimePoint> last_book_timestamp_by_instrument_;
};
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
  // Revert to 4 dispatcher threads for better performance. The handler is now robust.
  ccapi::EventDispatcher dispatcher(4);
  ccapi::Session session(sessionOptions, sessionConfigs, &eventHandler, &dispatcher);

  const std::vector<std::string> PAIRS_TO_SUBSCRIBE = {"BTC/USD",  "ETH/USD", "USDT/USD", "SOL/USD",  "XRP/USD",
                                                       "DOGE/USD", "ADA/USD", "LTC/USD",  "LINK/USD", "DOT/USD"};

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

    // L3 Subscription (goes to the authenticated endpoint)
    ccapi::Subscription l3Sub("market_data_l3",  // Custom service for ws-auth
                              CCAPI_EXCHANGE_NAME_KRAKEN, symbol, ccapi::CCAPI_KRAKEN_FIELD_LEVEL3, "depth=10", "L3_" + canonicalPair, {{"token", wsToken}});
    mdL3Subscriptions.push_back(l3Sub);
    eventHandler.addSubscriptionDetails("L3_" + canonicalPair, CCAPI_EXCHANGE_NAME_KRAKEN, symbol);

    // Public Trade Subscription (goes to the public endpoint)
    ccapi::Subscription publicTradeSub(CCAPI_EXCHANGE_NAME_KRAKEN,                                   // Default service for ws
                                       symbol, CCAPI_TRADE, "", "PUBLIC_TRADE_" + canonicalPair, {}  // No token needed
    );
    mdTradeSubscriptions.push_back(publicTradeSub);
    eventHandler.addSubscriptionDetails("PUBLIC_TRADE_" + canonicalPair, CCAPI_EXCHANGE_NAME_KRAKEN, symbol);
  }

  if (!mdL3Subscriptions.empty()) {
    session.subscribe(mdL3Subscriptions);
  }
  if (!mdTradeSubscriptions.empty()) {
    session.subscribe(mdTradeSubscriptions);
  }

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
