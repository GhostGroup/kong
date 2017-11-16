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


local get_consumer_id = {
  consumer_id = function(consumer)
    return consumer and string_gsub(consumer.id, "-", "_")
  end,
  custom_id   = function(consumer)
    return consumer and consumer.custom_id
  end,
  username    = function(consumer)
    return consumer and consumer.username
  end
}


local metrics = {
  status_count = function (name_prefix, message, metric_config, logger, tags)
    local fmt = string_format("%srequest.status", name_prefix,
                       message.response.status)

    logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                       1, logger.stat_types.counter,
                       metric_config.sample_rate, tags)

    logger:send_statsd(string_format("%s.%s", fmt, "total"), 1,
                       logger.stat_types.counter,
                       metric_config.sample_rate, tags)
  end,
  unique_users = function (name_prefix, message, metric_config, logger, tags)
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local stat = string_format("%suser.uniques", name_prefix)

      logger:send_statsd(stat, consumer_id, logger.stat_types.set, nil, tags)
    end
  end,
  request_per_user = function (name_prefix, message, metric_config, logger, tags)
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local stat = string_format("%suser.%s.request.count", name_prefix,
                                 consumer_id)

      logger:send_statsd(stat, 1, logger.stat_types.counter,
                         metric_config.sample_rate, tags)
    end
  end,
  status_count_per_user = function (name_prefix, message, metric_config, logger,
                                    tags)
    local get_consumer_id = get_consumer_id[metric_config.consumer_identifier]
    local consumer_id     = get_consumer_id(message.consumer)

    if consumer_id then
      local fmt = string_format("%suser.%s.request.status", name_prefix,
                                consumer_id)

      logger:send_statsd(string_format("%s.%s", fmt, message.response.status),
                         1, logger.stat_types.counter,
                         metric_config.sample_rate, tags)

      logger:send_statsd(string_format("%s.%s", fmt, "total"),
                         1, logger.stat_types.counter,
                         metric_config.sample_rate, tags)
    end
  end,
}


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
  local name_prefix = conf.tag_api_name and "" or api_name .. "."
  local stat_name   = {
    request_size     = name_prefix .. "request.size",
    response_size    = name_prefix .. "response.size",
    latency          = name_prefix .. "latency",
    upstream_latency = name_prefix .. "upstream_latency",
    kong_latency     = name_prefix .. "kong_latency",
    request_count    = name_prefix .. "request.count",
  }
  local stat_value = {
    request_size     = message.request.size,
    response_size    = message.response.size,
    latency          = message.latencies.request,
    upstream_latency = message.latencies.proxy,
    kong_latency     = message.latencies.kong,
    request_count    = 1,
  }

  for _, metric_config in pairs(conf.metrics) do
    local metric = metrics[metric_config.name]

    local tags = metric_config.tags or {}
    if conf.tag_api_name then
      table.insert(tags, "api_name:" .. api_name)
    end

    if metric then
      metric(name_prefix, message, metric_config, logger, tags)
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
