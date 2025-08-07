#ifndef INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_KRAKEN_H_
#define INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_KRAKEN_H_
#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_KRAKEN
#include <charconv>  // For std::to_chars
#include <iostream>
#include <string>  // For std::string

#include "ccapi_cpp/ccapi_subscription.h"  // Add this include for the Subscription class
#include "ccapi_cpp/service/ccapi_market_data_service.h"
#include "json_utils.h"       // safeGetString()
#include "nlohmann/json.hpp"  // ANNOTATION: Added for easier parsing of complex L3 messages.
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace ccapi {

// ANNOTATION: Added a constant for the L3 field name used in your C++ application.
// This is an internal identifier within CCAPI for your L3 subscriptions.
static constexpr const char* CCAPI_KRAKEN_SERVICE_NAME_MARKET_DATA_L3 = "market_data_l3";
inline const char* CCAPI_KRAKEN_FIELD_LEVEL3 = "level3";
// ANNOTATION: Added a constant for the actual channel name Kraken uses for L3 data.
static constexpr const char* CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3 = "level3";

class MarketDataServiceKraken : public MarketDataService {
 public:
  MarketDataServiceKraken(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                          ServiceContext* serviceContextPtr)
      : MarketDataService(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
    this->exchangeName = CCAPI_EXCHANGE_NAME_KRAKEN;
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
  }

  void unsubscribe(const std::vector<Subscription>& subscriptionList) override {
    // This function will now ONLY send the message.
    CCAPI_LOGGER_INFO("MarketDataServiceKraken::unsubscribe override called to SEND message.");
    for (const auto& sub : subscriptionList) {
      // The logic is now perfect for single-instrument unsubscription
      // because the subscriptionList will only contain the one we need to remove.
      if (sub.getField() == CCAPI_KRAKEN_FIELD_LEVEL3) {
        this->sendUnsubscribe(sub);
      }
    }
    // We call the base class unsubscribe to handle removing the subscription
    // from the internal tracking lists.
    MarketDataService::unsubscribe(subscriptionList);
  }

  void sendUnsubscribe(const Subscription& subscription) {
    CCAPI_LOGGER_INFO("Executing custom sendUnsubscribe for " + subscription.getCorrelationId());

    // Add this debug print to see which host the service *thinks* it should be using
    std::cerr << "[DEBUG] sendUnsubscribe called. Current service hostWs is: " << this->hostWs << std::endl;

    std::shared_ptr<WsConnection> wsConnectionPtr = nullptr;

    for (const auto& it : this->wsConnectionByIdMap) {
      auto connection = it.second;

      std::cerr << "[DEBUG] sendUnsubscribe: Checking connection with host " << connection->host << " and url " << connection->url << std::endl;

      for (const auto& sub : connection->subscriptionList) {
        if (sub.getCorrelationId() == subscription.getCorrelationId()) {
          wsConnectionPtr = connection;
          std::cerr << "[DEBUG] sendUnsubscribe: Found matching connection for CorrID " << subscription.getCorrelationId()
                    << ". Host: " << wsConnectionPtr->host << std::endl;
          break;
        }
      }
      if (wsConnectionPtr) break;
    }

    if (!wsConnectionPtr) {
      CCAPI_LOGGER_ERROR("Could not find WsConnection for unsubscribe request with CorrID: " + subscription.getCorrelationId());
      return;
    }

    rj::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    doc.AddMember("method", rj::Value("unsubscribe").Move(), allocator);

    const auto& optionMap = subscription.getOptionMap();
    if (optionMap.count("req_id")) {
      const std::string& req_id_str = optionMap.at("req_id");
      try {
        long long req_id_num = std::stoll(req_id_str);
        doc.AddMember("req_id", req_id_num, allocator);
      } catch (const std::exception&) {
        CCAPI_LOGGER_WARN("req_id '" + req_id_str + "' is not a valid number and will not be sent.");
      }
    }

    rj::Value params(rj::kObjectType);
    params.AddMember("channel", rj::Value(CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3, allocator), allocator);

    rj::Value symbolArray(rj::kArrayType);
    std::string instrument = subscription.getInstrument();
    symbolArray.PushBack(rj::Value(instrument.c_str(), allocator), allocator);
    params.AddMember("symbol", symbolArray, allocator);

    if (optionMap.count("depth")) {
      try {
        int depth = std::stoi(optionMap.at("depth"));
        params.AddMember("depth", depth, allocator);
      } catch (const std::exception& e) {
        CCAPI_LOGGER_WARN("Could not parse 'depth' from subscription options: " + std::string(e.what()));
      }
    }

    if (subscription.getCredential().count("token")) {
      std::string token = subscription.getCredential().at("token");
      params.AddMember("token", rj::Value(token.c_str(), allocator), allocator);
    }
    doc.AddMember("params", params, allocator);

    rj::StringBuffer buf;
    rj::Writer<rj::StringBuffer> w(buf);
    doc.Accept(w);
    std::string sendString = buf.GetString();

    std::cerr << "[DEBUG] SENDING UNSUBSCRIBE JSON to " << wsConnectionPtr->url << ": " << sendString << std::endl;
    ErrorCode ec;
    this->send(wsConnectionPtr, sendString, ec);
    if (ec) {
      this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "unsubscribe");
    }
  }

  void subscribe(std::vector<Subscription>& subscriptionList) override {
    bool isL3Service =
        std::any_of(subscriptionList.begin(), subscriptionList.end(), [](const Subscription& sub) { return sub.getServiceName() == "market_data_l3"; });

    std::string newUrl = isL3Service ? "wss://ws-auth.kraken.com/v2" : "wss://ws.kraken.com/v2";

    if (this->baseUrlWs != newUrl && !this->baseUrlWs.empty()) {
      CCAPI_LOGGER_INFO("Endpoint changing from " + this->baseUrlWs + " to " + newUrl + ". Closing stale connections.");
      std::vector<std::shared_ptr<WsConnection>> connectionsToClose;
      for (const auto& it : this->wsConnectionByIdMap) {
        if (it.second->host == this->hostWs) {
          connectionsToClose.push_back(it.second);
        }
      }
      for (auto& conn : connectionsToClose) {
        ErrorCode ec;
        this->close(conn, beast::websocket::close_code::normal, beast::websocket::close_reason("endpoint switch"), ec);
      }
    }
    this->baseUrlWs = newUrl;
    this->setHostWsFromUrlWs(this->baseUrlWs);

    std::shared_ptr<WsConnection> wsConnectionPtr = nullptr;
    for (const auto& it : this->wsConnectionByIdMap) {
      if (it.second->host == this->hostWs) {
        wsConnectionPtr = it.second;
        break;
      }
    }

    if (wsConnectionPtr && wsConnectionPtr->status == WsConnection::Status::OPEN) {
      CCAPI_LOGGER_INFO("Connection for " + this->hostWs + " is already open. Manually sending subscribe and updating state.");

      for (const auto& sub : subscriptionList) {
        wsConnectionPtr->subscriptionList.push_back(sub);
      }

      std::map<std::string, std::vector<std::string>> subsByChannel;
      for (const auto& sub : subscriptionList) {
        std::string channelName;
        if (sub.getField() == CCAPI_KRAKEN_FIELD_LEVEL3) {
          channelName = CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3;
        } else if (sub.getField() == CCAPI_TRADE) {
          channelName = "trade";
        }
        if (!channelName.empty()) {
          subsByChannel[channelName].push_back(sub.getInstrument());
        }
      }

      rj::Document docTemplate;
      docTemplate.SetObject();
      docTemplate.AddMember("method", rj::Value("subscribe").Move(), docTemplate.GetAllocator());

      for (const auto& pair : subsByChannel) {
        rj::Document doc;
        doc.CopyFrom(docTemplate, doc.GetAllocator());
        auto& a = doc.GetAllocator();

        const std::string& channelName = pair.first;
        const std::vector<std::string>& symbols = pair.second;

        rj::Value params(rj::kObjectType);
        params.AddMember("channel", rj::Value(channelName.c_str(), a).Move(), a);

        rj::Value symbolArray(rj::kArrayType);
        for (const auto& symbol : symbols) {
          symbolArray.PushBack(rj::Value(symbol.c_str(), a).Move(), a);
        }
        params.AddMember("symbol", symbolArray, a);

        if (channelName == CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3) {
          const auto& sub = subscriptionList.front();
          const auto& credential = sub.getCredential();
          if (credential.count("token")) {
            params.AddMember("token", rj::Value(credential.at("token").c_str(), a).Move(), a);
          }
          const auto& optionMap = sub.getOptionMap();
          if (optionMap.count("depth")) {
            try {
              int depth = std::stoi(optionMap.at("depth"));
              params.AddMember("depth", depth, a);
            } catch (const std::exception& e) {
            }
          }
          params.AddMember("snapshot", rj::Value(true).Move(), a);
        }

        doc.AddMember("params", params, a);

        rj::StringBuffer buf;
        rj::Writer<rj::StringBuffer> w(buf);
        doc.Accept(w);
        std::string sendString = buf.GetString();

        ErrorCode ec;
        this->send(wsConnectionPtr, sendString, ec);
        if (ec) {
          this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "re-subscribe");
        }
      }
    } else {
      CCAPI_LOGGER_INFO("No open connection found for " + this->hostWs + ". Deferring to base class subscribe logic.");
      MarketDataService::subscribe(subscriptionList);
    }
  }

  virtual ~MarketDataServiceKraken() {}

#ifndef CCAPI_EXPOSE_INTERNAL
 private:
#endif

  void onTextMessage(std::shared_ptr<WsConnection> wsConnectionPtr, boost::beast::string_view textMessageView, const TimePoint& timeReceived) override {
    std::string textMessage(textMessageView);
    // std::cerr << "[WS RAW INBOUND] " << textMessage << std::endl;
    rj::Document d;
    d.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());

    if (d.HasParseError()) {
      CCAPI_LOGGER_WARN("Failed to parse Kraken message as JSON: " + textMessage);
      MarketDataService::onTextMessage(wsConnectionPtr, boost::beast::string_view(textMessage), timeReceived);
      return;
    }

    std::string channel = d.HasMember("channel") && d["channel"].IsString() ? d["channel"].GetString() : "";
    std::string method = d.HasMember("method") && d["method"].IsString() ? d["method"].GetString() : "";

    Event event;
    std::vector<Message> messageList;
    // --- NEW: Unsubscribe ACK → SUBSCRIPTION_STATUS ---
    if (method == "subscribe" || method == "unsubscribe") {
      Event event;
      event.setType(Event::Type::SUBSCRIPTION_STATUS);
      Message message;
      message.setTimeReceived(timeReceived);

      if (d.HasMember("req_id") && d["req_id"].IsInt64()) {
        long long req_id_num = d["req_id"].GetInt64();
        message.setCorrelationIdList({std::to_string(req_id_num)});
      }
      bool success = d.HasMember("success") && d["success"].IsBool() && d["success"].GetBool();

      if (method == "subscribe") {
        message.setType(success ? Message::Type::SUBSCRIPTION_STARTED : Message::Type::SUBSCRIPTION_FAILURE);
      } else {  // method == "unsubscribe"
        message.setType(success ? Message::Type::SUBSCRIPTION_ENDED : Message::Type::SUBSCRIPTION_FAILURE);
      }

      Element element;
      element.insert(success ? CCAPI_INFO_MESSAGE : CCAPI_ERROR_MESSAGE, textMessage);
      message.setElementList({element});
      event.setMessageList({message});
      this->eventHandler(event, nullptr);
      return;

    } else if (channel == "level3") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);

      // FIX: This flag MUST be determined at the message level, not outside.
      // The 'type' field is a top-level property of the JSON message.
      bool isSnapshot = (d.HasMember("type") && d["type"].IsString() && std::string(d["type"].GetString()) == "snapshot");

      if (d.HasMember("data") && d["data"].IsArray()) {
        for (const auto& data_item : d["data"].GetArray()) {
          if (!data_item.IsObject() || !data_item.HasMember("symbol")) continue;

          // MOVED: Create a fresh message and element list for EACH item in the data array.
          Message message;
          std::vector<Element> elementList;

          message.setType(Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH);
          message.setTimeReceived(timeReceived);
          message.setRecapType(isSnapshot ? Message::RecapType::SOLICITED : Message::RecapType::NONE);

          if (data_item.HasMember("timestamp") && data_item["timestamp"].IsString()) {
            message.setTime(ccapi::UtilTime::parse(data_item["timestamp"].GetString()));
          } else {
            message.setTime(timeReceived);
          }

          const std::string& receivedSymbol = data_item["symbol"].GetString();
          for (const auto& sub : wsConnectionPtr->subscriptionList) {
            if (sub.getInstrument() == receivedSymbol && sub.getField() == "level3") {
              message.setCorrelationIdList({sub.getCorrelationId()});
              break;
            }
          }

          std::string checksum_str;
          if (data_item.HasMember("checksum") && data_item["checksum"].IsString()) {
            checksum_str = data_item["checksum"].GetString();
          }

          // The processOrders lambda now correctly populates the per-item 'elementList'
          auto processOrders = [&](const char* sideKey, const char* sideValue) {
            if (data_item.HasMember(sideKey) && data_item[sideKey].IsArray()) {
              for (const auto& order : data_item[sideKey].GetArray()) {
                if (!order.IsObject() || !order.HasMember("order_id")) continue;

                Element element;
                element.insert(CCAPI_EM_ORDER_ID, order["order_id"].GetString());
                element.insert(CCAPI_EM_ORDER_SIDE, sideValue);
                element.insert("event_time", order["timestamp"].GetString());
                if (!checksum_str.empty()) {
                  element.insert("checksum", checksum_str);
                }

                if (!isSnapshot) {
                  if (order.HasMember("event") && order["event"].IsString()) {
                    std::string event_type = order["event"].GetString();
                    element.insert("event", event_type);
                    if (event_type == "delete") {
                      elementList.push_back(element);
                      continue;
                    }
                  }
                }

                element.insert(CCAPI_EM_ORDER_LIMIT_PRICE, order["limit_price"].GetString());
                element.insert(CCAPI_EM_ORDER_QUANTITY, order["order_qty"].GetString());
                elementList.push_back(element);
              }
            }
          };

          processOrders("asks", "ask");
          processOrders("bids", "bid");

          // MOVED: Set elements and push the completed message inside the loop.
          if (!elementList.empty()) {
            message.setElementList(elementList);
            messageList.push_back(message);
          }
        }
      }
    } else if (channel == "trade") {
      event.setType(Event::Type::SUBSCRIPTION_DATA);
      if (d.HasMember("data") && d["data"].IsArray()) {
        Message message;
        message.setType(Message::Type::MARKET_DATA_EVENTS_TRADE);
        message.setTimeReceived(timeReceived);

        std::vector<Element> elementList;
        for (const auto& trade_item : d["data"].GetArray()) {
          if (!trade_item.IsObject() || !trade_item.HasMember("symbol")) continue;

          std::string priceStr = trade_item["price"].GetString();
          std::string qtyStr = trade_item["qty"].GetString();

          if (std::stod(priceStr) <= 0 || std::stod(qtyStr) <= 0) {
            CCAPI_LOGGER_WARN("KRAKEN SENT ZERO-VALUE TRADE: " + textMessage);
            continue;
          }

          if (message.getCorrelationIdList().empty()) {
            const std::string& receivedSymbol = trade_item["symbol"].GetString();
            for (const auto& sub : wsConnectionPtr->subscriptionList) {
              if (sub.getInstrument() == receivedSymbol && sub.getField() == CCAPI_TRADE) {
                message.setCorrelationIdList({sub.getCorrelationId()});
                break;
              }
            }
          }

          Element element;

          // SIMPLIFIED LOGIC: Just pass the raw, high-precision timestamp string directly.
          if (trade_item.HasMember("timestamp") && trade_item["timestamp"].IsString()) {
            element.insert("event_time", trade_item["timestamp"].GetString());
          }

          element.insert(CCAPI_LAST_PRICE, priceStr);
          element.insert(CCAPI_LAST_SIZE, qtyStr);
          element.insert(CCAPI_EM_ORDER_SIDE, trade_item["side"].GetString());
          if (trade_item.HasMember("ord_type")) {
            element.insert("ORD_TYPE", trade_item["ord_type"].GetString());
          }
          elementList.push_back(element);
        }

        if (!elementList.empty()) {
          message.setElementList(elementList);
          messageList.push_back(message);
        }
      }
    }

    if (!messageList.empty()) {
      event.setMessageList(messageList);
      this->eventHandler(event, nullptr);
    }
    return;

    CCAPI_LOGGER_WARN("Received and discarded unhandled message on Kraken MD WS: " + textMessage);
  }

  void pingOnApplicationLevel(std::shared_ptr<WsConnection> wsConnectionPtr, ErrorCode& ec) override {
    this->send(wsConnectionPtr, R"({"method":"ping"})", ec);
  }

  bool doesHttpBodyContainError(const std::string& body) override { return body.find(R"("error":[])") == std::string::npos; }

  // This version ensures all subscriptions (L3 and public TRADE) are grouped
  // onto a single connection by assigning them the same virtual channel ID.
  void prepareSubscriptionDetail(std::string& channelId, std::string& symbolId, const std::string& field, const WsConnection& wsConnection,
                                 const Subscription& subscription, const std::map<std::string, std::string> optionMap) override {
    if (field == CCAPI_KRAKEN_FIELD_LEVEL3) {
      // We still need to store the depth for the subscription message.
      int depth = std::stoi(optionMap.at("depth"));
      this->marketDepthSubscribedToExchangeByConnectionIdChannelIdSymbolIdMap[wsConnection.id][channelId][symbolId] = depth;
    }
  }

  // In ccapi_market_data_service_kraken.h

  std::vector<std::string> createSendStringList(const WsConnection& wsConnection) override {
    std::map<std::string, std::vector<std::string>> subsByChannel;
    for (const auto& sub : wsConnection.subscriptionList) {
      std::string channelName;
      if (sub.getField() == CCAPI_KRAKEN_FIELD_LEVEL3) {
        channelName = CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3;  // "level3"
      } else if (sub.getField() == CCAPI_TRADE) {
        channelName = "trade";
      }
      if (!channelName.empty()) {
        subsByChannel[channelName].push_back(sub.getInstrument());
      }
    }

    std::vector<std::string> sendStringList;
    if (subsByChannel.empty()) {
      return sendStringList;
    }

    rj::Document docTemplate;
    docTemplate.SetObject();
    docTemplate.AddMember("method", rj::Value("subscribe").Move(), docTemplate.GetAllocator());

    for (const auto& pair : subsByChannel) {
      const std::string& channelName = pair.first;
      const std::vector<std::string>& symbols = pair.second;

      rj::Document doc;
      doc.CopyFrom(docTemplate, doc.GetAllocator());
      auto& a = doc.GetAllocator();

      rj::Value params(rj::kObjectType);
      params.AddMember("channel", rj::Value(channelName.c_str(), a).Move(), a);

      rj::Value symbolArray(rj::kArrayType);
      for (const auto& symbol : symbols) {
        symbolArray.PushBack(rj::Value(symbol.c_str(), a).Move(), a);
      }
      params.AddMember("symbol", symbolArray, a);

      // The token and snapshot flags are ONLY for the L3 channel.
      if (channelName == CCAPI_WEBSOCKET_KRAKEN_CHANNEL_BOOK_L3) {
        // Find a subscription to get the token from.
        // It's guaranteed to be an L3 subscription on this connection.
        for (const auto& sub : wsConnection.subscriptionList) {
          if (sub.getField() == CCAPI_KRAKEN_FIELD_LEVEL3) {
            const auto& credential = sub.getCredential();
            if (credential.count("token")) {
              params.AddMember("token", rj::Value(credential.at("token").c_str(), a).Move(), a);
            }
            const auto& optionMap = sub.getOptionMap();
            if (optionMap.count("depth")) {
              try {
                int depth = std::stoi(optionMap.at("depth"));
                params.AddMember("depth", depth, a);
              } catch (const std::exception& e) {
                // Handle error if "depth" is not a valid integer
              }
            }
            break;  // Found one L3 sub, that's enough
          }
        }
        params.AddMember("snapshot", rj::Value(true).Move(), a);
      }

      doc.AddMember("params", params, a);

      rj::StringBuffer buf;
      rj::Writer<rj::StringBuffer> w(buf);
      doc.Accept(w);
      sendStringList.push_back(buf.GetString());
    }

    return sendStringList;
  }

  void onOpen(std::shared_ptr<WsConnection> wsConnectionPtr) override {
    CCAPI_LOGGER_FUNCTION_ENTER;
    Service::onOpen(wsConnectionPtr);
    std::vector<std::string> sendStringList = this->createSendStringList(*wsConnectionPtr);
    for (const auto& sendString : sendStringList) {
      CCAPI_LOGGER_INFO("Sending MD Subscription: " + sendString);
      ErrorCode ec;
      this->send(wsConnectionPtr, sendString, ec);
      if (ec) {
        this->onError(Event::Type::SUBSCRIPTION_STATUS, Message::Type::SUBSCRIPTION_FAILURE, ec, "subscribe");
      }
    }
  }

  void convertRequestForRest(http::request<http::string_body>& req, const Request& request, const TimePoint& now, const std::string& symbolId,
                             const std::map<std::string, std::string>& credential) override {
    switch (request.getOperation()) {
      case Request::Operation::GENERIC_PUBLIC_REQUEST: {
        MarketDataService::convertRequestForRestGenericPublicRequest(req, request, now, symbolId, credential);
      } break;
      case Request::Operation::GET_RECENT_TRADES: {
        req.method(http::verb::get);
        auto target = this->getRecentTradesTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendSymbolId(queryString, symbolId, "pair");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_INSTRUMENT: {
        req.method(http::verb::get);
        auto target = this->getInstrumentTarget;
        std::string queryString;
        const std::map<std::string, std::string> param = request.getFirstParamWithDefault();
        this->appendSymbolId(queryString, symbolId, "pair");
        req.target(target + "?" + queryString);
      } break;
      case Request::Operation::GET_INSTRUMENTS: {
        req.method(http::verb::get);
        auto target = this->getInstrumentsTarget;
        req.target(target);
      } break;
      default:
        this->convertRequestForRestCustom(req, request, now, symbolId, credential);
    }
  }

  void extractInstrumentInfo(Element& element, const rj::Value& x) {
    element.insert(CCAPI_BASE_ASSET, x["base"].GetString());
    element.insert(CCAPI_QUOTE_ASSET, x["quote"].GetString());
    int pairDecimals = std::stoi(x["pair_decimals"].GetString());
    if (pairDecimals > 0) {
      element.insert(CCAPI_ORDER_PRICE_INCREMENT, "0." + std::string(pairDecimals - 1, '0') + "1");
    } else {
      element.insert(CCAPI_ORDER_PRICE_INCREMENT, "1");
    }
    int lotDecimals = std::stoi(x["lot_decimals"].GetString());
    element.insert(CCAPI_ORDER_QUANTITY_INCREMENT, "0." + std::string(lotDecimals - 1, '0') + "1");
    element.insert(CCAPI_ORDER_QUANTITY_MIN, x["ordermin"].GetString());
  }

  void convertTextMessageToMarketDataMessage(const Request& request, const std::string& textMessage, const TimePoint& timeReceived, Event& event,
                                             std::vector<MarketDataMessage>& marketDataMessageList) override {
    rj::Document document;
    document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
    auto instrument = request.getInstrument();
    const std::string& symbolId = instrument;
    switch (request.getOperation()) {
      case Request::Operation::GET_RECENT_TRADES: {
        for (const auto& x : document["result"][symbolId.c_str()].GetArray()) {
          MarketDataMessage marketDataMessage;
          marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE;
          auto timePair = UtilTime::divide(std::string(x[2].GetString()));
          auto tp = TimePoint(std::chrono::duration<int64_t>(timePair.first));
          tp += std::chrono::nanoseconds(timePair.second);
          marketDataMessage.tp = tp;
          MarketDataMessage::TypeForDataPoint dataPoint;
          dataPoint.insert({MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalString(std::string(x[0].GetString()))});
          dataPoint.insert({MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalString(std::string(x[1].GetString()))});
          dataPoint.insert({MarketDataMessage::DataFieldType::IS_BUYER_MAKER, std::string(x[3].GetString()) == "s" ? "1" : "0"});
          marketDataMessage.data[MarketDataMessage::DataType::TRADE].emplace_back(std::move(dataPoint));
          marketDataMessageList.emplace_back(std::move(marketDataMessage));
        }
      } break;
      case Request::Operation::GET_INSTRUMENT: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        const rj::Value& x = document["result"][request.getInstrument().c_str()];
        Element element;
        this->extractInstrumentInfo(element, x);
        element.insert(CCAPI_INSTRUMENT, request.getInstrument());
        message.setElementList({element});
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      case Request::Operation::GET_INSTRUMENTS: {
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
        std::vector<Element> elementList;
        for (auto itr = document["result"].MemberBegin(); itr != document["result"].MemberEnd(); ++itr) {
          Element element;
          this->extractInstrumentInfo(element, itr->value);
          element.insert(CCAPI_INSTRUMENT, itr->name.GetString());
          elementList.push_back(element);
        }
        message.setElementList(elementList);
        message.setCorrelationIdList({request.getCorrelationId()});
        event.addMessages({message});
      } break;
      default:
        CCAPI_LOGGER_FATAL(CCAPI_UNSUPPORTED_VALUE);
    }
  }
};
} /* namespace ccapi */
#endif
#endif
#endif  // INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_KRAKEN_H_
