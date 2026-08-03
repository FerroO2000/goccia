local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'http';

local reqDurCount = "http_server_request_duration_seconds_count";
local reqDurCountField = prometheus.field(reqDurCount);

local reqDurBucketsField = prometheus.field("http_server_request_duration_seconds_bucket");

local reqBodySizeField = prometheus.field("http_server_request_body_size_bytes_bucket");
local resBodySizeField = prometheus.field("http_server_response_body_size_bytes_bucket");

l.row(
  common.getTitle('HTTP'),
  [
    p.stat.base('Error Rate', prometheus.percentage(
        prometheus.field(reqDurCount, [prometheus.filter.contains("http_response_status_code", "5..")]),
        reqDurCountField,
    )),

    p.stat.base("Active Requests", prometheus.summed(prometheus.field("http_server_active_requests")), unit="req"),
    
    p.stat.base("Pending Responses", prometheus.summed(prometheus.field("goccia_http_ingress_pending_responses")), unit="res"),

    p.timeSeries.requestRate("Request Rate", prometheus.summedRate(reqDurCountField, "exported_job", "{{exported_job}}")),
    p.timeSeries.requestRate("Request Rate By Method", prometheus.summedRate(reqDurCountField, "http_request_method", "{{http_request_method}}")),
  
    p.timeSeries.withUnit("Request Latency", [
      prometheus.quant(reqDurBucketsField, 99),
      prometheus.quant(reqDurBucketsField, 95),
      prometheus.quant(reqDurBucketsField, 75),
      prometheus.quant(reqDurBucketsField, 50),
    ], "s"),

    p.heatmap.base("Request Duration Heatmap", prometheus.heatmap(reqDurBucketsField), unit="s"),

    p.timeSeries.withUnit("Responses By Status Code", 
      prometheus.summedRate(reqDurCountField, "http_response_status_code", "{{http_response_status_code}}"),
      "req",
    ),

    p.timeSeries.withUnit("Request Body Size", [
      prometheus.quant(reqBodySizeField, 99),
      prometheus.quant(reqBodySizeField, 95),
      prometheus.quant(reqBodySizeField, 75),
      prometheus.quant(reqBodySizeField, 50),
    ], "bytes"),

    p.timeSeries.withUnit("Response Body Size", [
      prometheus.quant(resBodySizeField, 99),
      prometheus.quant(resBodySizeField, 95),
      prometheus.quant(resBodySizeField, 75),
      prometheus.quant(resBodySizeField, 50),
    ], "bytes"),
  ]
)
