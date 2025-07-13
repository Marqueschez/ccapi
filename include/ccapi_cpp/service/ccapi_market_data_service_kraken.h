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

  static constexpr int PRICE_PRECISION = 7;
  static constexpr int SIZE_PRECISION = 8;

  void subscribe(std::vector<Subscription>& subscriptionList) override {
    bool isL3Service =
        std::any_of(subscriptionList.begin(), subscriptionList.end(), [](const Subscription& sub) { return sub.getServiceName() == "market_data_l3"; });

    if (isL3Service) {
      this->baseUrlWs = "wss://ws-auth.kraken.com/v2";
    } else {
      this->baseUrlWs = "wss://ws.kraken.com/v2";
    }

    this->setHostWsFromUrlWs(this->baseUrlWs);
    MarketDataService::subscribe(subscriptionList);
  }

  std::string doubleToStringWithMaxPrecision(double value) {
    std::ostringstream out;
    // std::numeric_limits<double>::max_digits10 is the number of digits
    // needed to uniquely represent any double value.
    out << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
    return out.str();
  }

  virtual ~MarketDataServiceKraken() {}

#ifndef CCAPI_EXPOSE_INTERNAL
 private:
#endif

  void onTextMessage(std::shared_ptr<WsConnection> wsConnectionPtr, boost::beast::string_view textMessageView, const TimePoint& timeReceived) override {
    std::string textMessage(textMessageView);
    nlohmann::json a;
    try {
      a = nlohmann::json::parse(textMessage);
    } catch (const nlohmann::json::parse_error& e) {
      CCAPI_LOGGER_WARN("Failed to parse Kraken message as JSON: " + textMessage);
      MarketDataService::onTextMessage(wsConnectionPtr, textMessageView, timeReceived);
      return;
    }

    std::string channel = a.value("channel", "");
    std::string method = a.value("method", "");

    if (channel == "level3" || channel == "trade" || method == "subscribe") {
      Event event;
      std::vector<Message> messageList;

      if (method == "subscribe") {
        event.setType(Event::Type::SUBSCRIPTION_STATUS);
        Message message;
        message.setTimeReceived(timeReceived);
        message.setType(a.value("success", false) ? Message::Type::SUBSCRIPTION_STARTED : Message::Type::SUBSCRIPTION_FAILURE);
        Element element;
        element.insert(a.value("success", false) ? CCAPI_INFO_MESSAGE : CCAPI_ERROR_MESSAGE, textMessage);
        message.setElementList({element});
        messageList.push_back(message);

      } else if (channel == "level3") {
        event.setType(Event::Type::SUBSCRIPTION_DATA);
        if (a.contains("data") && a.is_object() && a.at("data").is_array()) {  // Safety check 'is_object'
          for (const auto& data_item : a.at("data")) {
            if (!data_item.is_object() || !data_item.contains("symbol")) continue;  // Safety check

            Message message;
            message.setType(Message::Type::MARKET_DATA_EVENTS_MARKET_DEPTH);
            message.setTimeReceived(timeReceived);
            message.setRecapType((a.at("type").get<std::string>() == "snapshot") ? Message::RecapType::SOLICITED : Message::RecapType::NONE);

            const std::string& receivedSymbol = data_item.at("symbol").get<std::string>();
            message.setTime(timeReceived);

            for (const auto& sub : wsConnectionPtr->subscriptionList) {
              if (sub.getInstrument() == receivedSymbol && sub.getField() == "level3") {
                message.setCorrelationIdList({sub.getCorrelationId()});
                break;
              }
            }

            std::vector<Element> elementList;
            auto processOrders = [&](const std::string& sideKey, const std::string& sideValue) {
              if (data_item.contains(sideKey) && data_item.at(sideKey).is_array()) {
                char conversion_buffer[64];

                for (const auto& order : data_item.at(sideKey)) {
                  if (!order.is_object()) continue;
                  double price = 0.0;
                  if (order.contains("price")) {
                    price = order["price"].get<double>();
                  } else if (order.contains("limit_price")) {
                    price = order["limit_price"].get<double>();
                  } else {
                    continue;  // Skip malformed order
                  }

                  double qty = 0.0;
                  if (order.contains("qty")) {
                    qty = order["qty"].get<double>();
                  } else if (order.contains("order_qty")) {
                    qty = order["order_qty"].get<double>();
                  }

                  if (!order.contains("order_id")) {
                    continue;
                  }

                  Element element;
                  element.insert("event", order.value("event", "add"));
                  element.insert(CCAPI_EM_ORDER_ID, order.at("order_id").get<std::string>());
                  element.insert(CCAPI_EM_ORDER_SIDE, sideValue);

                  // --- HIGH-PERFORMANCE CONVERSION FOR PRICE ---
                  auto [ptr_price, ec_price] =
                      std::to_chars(conversion_buffer, conversion_buffer + sizeof(conversion_buffer), price, std::chars_format::fixed, PRICE_PRECISION);
                  if (ec_price == std::errc()) {  // Check for success
                    element.insert(CCAPI_EM_ORDER_LIMIT_PRICE, std::string(conversion_buffer, ptr_price));
                  }

                  // --- HIGH-PERFORMANCE CONVERSION FOR QUANTITY ---
                  auto [ptr_qty, ec_qty] =
                      std::to_chars(conversion_buffer, conversion_buffer + sizeof(conversion_buffer), qty, std::chars_format::fixed, SIZE_PRECISION);
                  if (ec_qty == std::errc()) {  // Check for success
                    element.insert(CCAPI_EM_ORDER_QUANTITY, std::string(conversion_buffer, ptr_qty));
                  }

                  elementList.push_back(element);
                }
              }
            };
            processOrders("bids", "bid");
            processOrders("asks", "ask");

            if (!elementList.empty()) {
              message.setElementList(elementList);
              messageList.push_back(message);
            }
          }
        }

      } else if (channel == "trade") {
        event.setType(Event::Type::SUBSCRIPTION_DATA);
        if (a.contains("data") && a.at("data").is_array()) {
          Message message;
          message.setType(Message::Type::MARKET_DATA_EVENTS_TRADE);
          message.setTimeReceived(timeReceived);

          const auto& dataArray = a.at("data");
          TimePoint base_tp =
              (!dataArray.empty() && dataArray[0].contains("timestamp")) ? UtilTime::parse(dataArray[0].at("timestamp").get<std::string>()) : timeReceived;
          message.setTime(base_tp);

          std::vector<Element> elementList;
          for (const auto& trade_item : dataArray) {
            if (!trade_item.is_object() || !trade_item.contains("symbol")) continue;

            double price = trade_item.value("price", 0.0);
            double qty = trade_item.value("qty", 0.0);
            if (price <= 0 || qty <= 0) {
              CCAPI_LOGGER_WARN("KRAKEN SENT ZERO-VALUE TRADE: " + trade_item.dump());
              continue;
            }

            if (message.getCorrelationIdList().empty()) {
              const std::string& receivedSymbol = trade_item.at("symbol").get<std::string>();
              for (const auto& sub : wsConnectionPtr->subscriptionList) {
                if (sub.getInstrument() == receivedSymbol && sub.getField() == CCAPI_TRADE) {
                  message.setCorrelationIdList({sub.getCorrelationId()});
                  break;
                }
              }
            }

            Element element;
            element.insert(CCAPI_LAST_PRICE, doubleToStringWithMaxPrecision(price));
            element.insert(CCAPI_LAST_SIZE, doubleToStringWithMaxPrecision(qty));
            element.insert(CCAPI_EM_ORDER_SIDE, trade_item.at("side").get<std::string>());
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

      // CRITICAL FIX: Prevent this message from being processed again by the base class.
      return;
    }
    // For any other message type, pass it to the base class handler
    CCAPI_LOGGER_WARN("Received and discarded unhandled message on Kraken MD WS: " + textMessage);
    return;
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
