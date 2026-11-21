// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "private/eventhubs_diagnostics.hpp"
#include "private/eventhubs_metrics.hpp"
#include "private/eventhubs_utilities.hpp"

#include <azure/messaging/eventhubs.hpp>

#include <gtest/gtest.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/metric_reader.h>

namespace Azure { namespace Messaging { namespace EventHubs { namespace Test {
  namespace {
    namespace otel = opentelemetry;
    namespace metrics = otel::sdk::metrics;

    class Reader final : public metrics::MetricReader {
      bool OnForceFlush(std::chrono::microseconds) noexcept override { return true; }
      bool OnShutDown(std::chrono::microseconds) noexcept override { return true; }

    public:
      metrics::AggregationTemporality GetAggregationTemporality(
          metrics::InstrumentType) const noexcept override
      {
        return metrics::AggregationTemporality::kCumulative;
      }
    };
  } // namespace

  TEST(EventHubsMetricsTest, ReplacingGlobalProviderReleasesPreviousProviderWhileIdle)
  {
    auto previous = otel::metrics::Provider::GetMeterProvider();
    auto provider = std::make_shared<metrics::MeterProvider>();
    std::weak_ptr<metrics::MeterProvider> weak = provider;
    otel::metrics::Provider::SetMeterProvider(
        std::static_pointer_cast<otel::metrics::MeterProvider>(provider));
    _detail::RecordLifecycleMetric("link", "attached");
    otel::metrics::Provider::SetMeterProvider(previous);
    provider.reset();
    EXPECT_TRUE(weak.expired());
  }

  TEST(EventHubsMetricsTest, CountsPublicSendFailuresReturnedEventsAndTerminalLifecycle)
  {
    auto previous = otel::metrics::Provider::GetMeterProvider();
    auto provider = std::make_shared<metrics::MeterProvider>();
    auto reader = std::make_shared<Reader>();
    provider->AddMetricReader(reader);
    otel::metrics::Provider::SetMeterProvider(
        std::static_pointer_cast<otel::metrics::MeterProvider>(provider));

    std::string const hub{"test-hub"};
    std::string const host{"unused.invalid"};
    ProducerClient producer{
        "Endpoint=sb://unused.invalid/;SharedAccessKeyName=name;SharedAccessKey=key", hub};
    EventDataBatchOptions options;
    options.MaxBytes = 65536;
    auto batch = _detail::EventDataBatchFactory::CreateEventDataBatch(options);
    EXPECT_THROW(producer.Send(batch), std::runtime_error);
    Azure::Core::Context cancelled;
    cancelled.Cancel();
    // Batch conversion fails before cancellation is checked.
    EXPECT_THROW(producer.Send(batch, cancelled), std::runtime_error);
    EXPECT_TRUE(batch.TryAdd(Models::EventData{"event"}));
    EXPECT_THROW(producer.Send(batch, cancelled), Azure::Core::OperationCancelledException);

    {
      _detail::OperationMetrics receive{"receive", hub, host};
      receive.Complete(3);
    }
    {
      _detail::OperationMetrics receive{"receive", hub, host};
      receive.Complete(2, true);
    }
    _detail::AmqpDiagnosticsContext component{"client-identity", "0", "link", "name", 42};
    _detail::LogAmqpLifecycle(
        Azure::Core::Diagnostics::Logger::Level::Verbose, component, "attaching");
    _detail::LogAmqpLifecycle(
        Azure::Core::Diagnostics::Logger::Level::Verbose, component, "attached");

    otel::metrics::Provider::SetMeterProvider(previous);
    std::map<std::string, std::int64_t> observations;
    EXPECT_TRUE(reader->Collect([&](metrics::ResourceMetrics& result) {
      for (auto const& scope : result.scope_metric_data_)
      {
        for (auto const& metric : scope.metric_data_)
        {
          for (auto const& point : metric.point_data_attr_)
          {
            auto key = metric.instrument_descriptor.name_;
            auto const& attributes = point.attributes;
            if (key == "az.eventhubs.amqp.lifecycle")
            {
              EXPECT_EQ(2u, attributes.size());
              key += "/" + otel::nostd::get<std::string>(attributes.at("component"));
            }
            else
            {
              EXPECT_EQ(5u, attributes.size());
              EXPECT_EQ(host, otel::nostd::get<std::string>(attributes.at("server.address")));
              EXPECT_EQ(
                  hub, otel::nostd::get<std::string>(attributes.at("messaging.destination.name")));
              key += "/" + otel::nostd::get<std::string>(attributes.at("operation"));
            }
            key += "/" + otel::nostd::get<std::string>(attributes.at("outcome"));
            if (metric.instrument_descriptor.name_ == "az.eventhubs.operation.duration")
            {
              observations[key] = static_cast<std::int64_t>(
                  otel::nostd::get<metrics::HistogramPointData>(point.point_data).count_);
            }
            else
            {
              observations[key] = otel::nostd::get<std::int64_t>(
                  otel::nostd::get<metrics::SumPointData>(point.point_data).value_);
            }
          }
        }
      }
      return true;
    }));
    EXPECT_EQ(2, observations["az.eventhubs.operations/send/error"]);
    EXPECT_EQ(1, observations["az.eventhubs.operations/send/cancelled"]);
    EXPECT_EQ(1, observations["az.eventhubs.operations/receive/success"]);
    EXPECT_EQ(2, observations["az.eventhubs.operation.duration/send/error"]);
    EXPECT_EQ(1, observations["az.eventhubs.operation.duration/send/cancelled"]);
    EXPECT_EQ(1, observations["az.eventhubs.operation.duration/receive/success"]);
    EXPECT_EQ(3, observations["az.eventhubs.events/receive/success"]);
    EXPECT_EQ(1, observations["az.eventhubs.operations/receive/cancelled"]);
    EXPECT_EQ(1, observations["az.eventhubs.operation.duration/receive/cancelled"]);
    EXPECT_EQ(2, observations["az.eventhubs.events/receive/cancelled"]);
    EXPECT_EQ(0u, observations.count("az.eventhubs.events/send/error"));
    EXPECT_EQ(0u, observations.count("az.eventhubs.events/send/cancelled"));
    EXPECT_EQ(1, observations["az.eventhubs.amqp.lifecycle/link/success"]);
  }
}}}} // namespace Azure::Messaging::EventHubs::Test
