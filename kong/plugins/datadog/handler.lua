local BasePlugin       = require "kong.plugins.base_plugin"
local basic_serializer = require "kong.plugins.log-serializers.basic"
local statsd_logger    = require "kong.plugins.datadog.statsd_logger"

local ngx_log       = ngx.log
local ngx_timer_at  = ngx.timer.at
local string_gsub   = string.gsub
local pairs         = pairs
local string_format = string.format
local NGX_ERR       = ngx.ERR

local DatadogHandler    = BasePlugin:extend()
DatadogHandler.PRIORITY = 10
DatadogHandler.VERSION = "0.1.0"

local consumer_id_functions = {
  consumer_id = function(consumer) return consumer and string_gsub(consumer.id, "-", "_") end,
  custom_id   = function(consumer) return consumer and consumer.custom_id end,
  username    = function(consumer) return consumer and consumer.username end
}

local function get_consumer_id(metric_config, message)
  local consumer_id_func = consumer_id_functions[metric_config.consumer_identifier]
  return consumer_id_func(message.consumer)
end

local metric_functions = {}

function metric_functions.status_count(name_prefix, message, metric_config, logger, tags)
  local fmt = string_format("%srequest.status", name_prefix, message.response.status)

  logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                     1, logger.stat_types.counter, metric_config.sample_rate, tags)

  logger:send_statsd(string_format("%s.%s", fmt, "total"), 1,
                     logger.stat_types.counter, metric_config.sample_rate, tags)
end

function metric_functions.unique_users(name_prefix, message, metric_config, logger, tags)
  local consumer_id = get_consumer_id(metric_config, message)
  if consumer_id then
    local stat = string_format("%suser.uniques", name_prefix)
    logger:send_statsd(stat, consumer_id, logger.stat_types.set, nil, tags)
  end
end

function metric_functions.request_per_user(name_prefix, message, metric_config, logger, tags)
  local consumer_id = get_consumer_id(metric_config, message)

  if consumer_id then
    local stat = string_format("%suser.%s.request.count", name_prefix, consumer_id)
    logger:send_statsd(stat, 1, logger.stat_types.counter, metric_config.sample_rate, tags)
  end
end

function metric_functions.status_count_per_user(name_prefix, message, metric_config, logger, tags)
  local consumer_id = get_consumer_id(metric_config, message)

  if consumer_id then
    local fmt = string_format("%suser.%s.request.status", name_prefix, consumer_id)

    logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                       1, logger.stat_types.counter, metric_config.sample_rate, tags)

    logger:send_statsd(string_format("%s.%s", fmt, "total"),
                       1, logger.stat_types.counter, metric_config.sample_rate, tags)
  end
end

local function merge_tags(tags, api_name)
  local api_tag = "api_name:" .. api_name
  if tags then
    tags = {unpack(tags)}
    tags[#tags+1] = api_tag
  else
    tags = { api_tag }
  end
  return tags
end

local function build_name_prefix(api_name, conf)
  if conf.tag_api_name then
    return ""
  else
    return api_name .. "."
  end
end

local function build_stat_names(name_prefix)
  return {
    request_size     = name_prefix .. "request.size",
    response_size    = name_prefix .. "response.size",
    latency          = name_prefix .. "latency",
    upstream_latency = name_prefix .. "upstream_latency",
    kong_latency     = name_prefix .. "kong_latency",
    request_count    = name_prefix .. "request.count",
  }
end

local function collect_stat_values(message)
  return {
    request_size     = message.request.size,
    response_size    = message.response.size,
    latency          = message.latencies.request,
    upstream_latency = message.latencies.proxy,
    kong_latency     = message.latencies.kong,
    request_count    = 1,
  }
end

local function log(premature, conf, message)
  if premature then
    return
  end

  local logger, err = statsd_logger:new(conf)
  if err then
    ngx_log(NGX_ERR, "failed to create Statsd logger: ", err)
    return
  end

  local api_name    = string_gsub(message.api.name, "%.", "_")
  local name_prefix = build_name_prefix(api_name, conf)
  local stat_name   = build_stat_names(name_prefix)
  local stat_value  = collect_stat_values(message)

  for _, metric_config in pairs(conf.metrics) do
    local metric_func = metric_functions[metric_config.name]

    local tags = metric_config.tags
    if conf.tag_api_name then
      tags = merge_tags(tags, api_name)
    end

    if metric_func then
      metric_func(name_prefix, message, metric_config, logger, tags)
    else
      local stat_name  = stat_name[metric_config.name]
      local stat_value = stat_value[metric_config.name]

      logger:send_statsd(stat_name, stat_value,
                         logger.stat_types[metric_config.stat_type],
                         metric_config.sample_rate, tags)
    end
  end

  logger:close_socket()
end

function DatadogHandler:new()
  DatadogHandler.super.new(self, "datadog")
end

function DatadogHandler:log(conf)
  DatadogHandler.super.log(self)

  -- unmatched apis are nil
  if not ngx.ctx.api then
    return
  end

  local message = basic_serializer.serialize(ngx)

  local ok, err = ngx_timer_at(0, log, conf, message)
  if not ok then
    ngx_log(NGX_ERR, "failed to create timer: ", err)
  end
end

return DatadogHandler
