#pragma once

#include <string>
#include <vector>

#include <json/value.h>

#include <yandex/taxi/userver/json_config/variable_map.hpp>

#include <engine/coro/pool_config.hpp>
#include <engine/ev/thread_pool_config.hpp>
#include <engine/task/task_processor_config.hpp>

#include "component_config.hpp"

namespace components {

struct ManagerConfig {
  engine::coro::PoolConfig coro_pool;
  engine::ev::ThreadPoolConfig event_thread_pool;
  std::vector<components::ComponentConfig> components;
  std::vector<engine::TaskProcessorConfig> task_processors;
  std::string default_task_processor;

  Json::Value json;  // the owner
  json_config::VariableMapPtr config_vars_ptr;

  static ManagerConfig ParseFromJson(
      Json::Value json, const std::string& name,
      json_config::VariableMapPtr config_vars_ptr);

  static ManagerConfig ParseFromString(const std::string&);
  static ManagerConfig ParseFromFile(const std::string& path);
};

}  // namespace components
