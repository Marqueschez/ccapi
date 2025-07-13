#pragma once
#include <iostream>
#include <nlohmann/json.hpp>

// Safely extract a string field, logging a warning if missing
inline std::string safeGetString(const nlohmann::json &j, const std::string &key, const std::string &contextMsg = "") {
  if (j.contains(key)) {
    return j[key].get<std::string>();
  } else {
    std::cerr << "[WARN] missing field '" << key << "' in " << contextMsg << ". JSON: " << j.dump() << std::endl;
    return "";
  }
}
