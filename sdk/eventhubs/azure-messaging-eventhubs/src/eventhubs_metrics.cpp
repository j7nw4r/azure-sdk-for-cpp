// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#include "private/eventhubs_metrics.hpp"

#include "private/package_version.hpp"

#include <memory>
#include <mutex>

#include <opentelemetry/metrics/provider.h>

namespace Azure { namespace Messaging { namespace EventHubs { namespace _detail {
  namespace {
    namespace otel = opentelemetry;

    struct Instruments final
    {
      otel::nostd::shared_ptr<otel::metrics::Meter> Meter;
      otel::nostd::unique_ptr<otel::metrics::Counter<std::uint64_t>> Operations;
      otel::nostd::unique_ptr<otel::metrics::Counter<std::uint64_t>> Events;
      otel::nostd::unique_ptr<otel::metrics::Histogram<double>> Duration;
      otel::nostd::unique_ptr<otel::metrics::Counter<std::uint64_t>> Lifecycle;

      explicit Instruments(otel::nostd::shared_ptr<otel::metrics::Meter> meter)
          : Meter(std::move(meter)), Operations(Meter->CreateUInt64Counter(
                                         "az.eventhubs.operations",
                                         "Completed logical send and receive operations",
                                         "{operation}")),
            Events(Meter->CreateUInt64Counter(
                "az.eventhubs.events",
                "Events in accepted sends and returned receives",
                "{event}")),
            Duration(Meter->CreateDoubleHistogram(
                "az.eventhubs.operation.duration",
                "Logical operation duration including retries",
                "s")),
            Lifecycle(Meter->CreateUInt64Counter(
                "az.eventhubs.amqp.lifecycle",
                "AMQP creation attempts and recovery invalidations",
                "{transition}"))
      {
      }
    };

    std::shared_ptr<Instruments> GetInstruments()
    {
      static std::mutex mutex;
      static std::shared_ptr<Instruments> cached;
      auto provider = otel::metrics::Provider::GetMeterProvider();
      if (!provider)
      {
        return {};
      }
      // Keep the instruments and their meter without extending the lifetime of the provider.
      auto meter = provider->GetMeter("azure-messaging-eventhubs-cpp", PackageVersion::ToString());
      std::lock_guard<std::mutex> guard(mutex);
      if (!cached || meter != cached->Meter)
      {
        cached = std::make_shared<Instruments>(std::move(meter));
      }
      return cached;
    }
  } // namespace

  OperationMetrics::~OperationMetrics()
  {
    auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - m_start);
    auto instruments = GetInstruments();
    if (!instruments)
    {
      return;
    }
    char const* outcome = m_succeeded ? "success" : "error";
    if (m_cancelled || (!m_succeeded && m_context.IsCancelled()))
    {
      outcome = "cancelled";
    }
    std::initializer_list<std::pair<otel::nostd::string_view, otel::common::AttributeValue>> labels{
        {"messaging.system", "eventhubs"},
        {"messaging.destination.name", otel::nostd::string_view{m_hub}},
        {"server.address", otel::nostd::string_view{m_host}},
        {"operation", m_operation},
        {"outcome", outcome}};
    instruments->Operations->Add(1, labels);
    instruments->Duration->Record(elapsed.count(), labels, {});
    if (m_events != 0)
    {
      instruments->Events->Add(m_events, labels);
    }
  }

  void RecordLifecycleMetric(std::string const& component, std::string const& event)
  {
    char const* outcome;
    if (event == "created" || event == "recreated" || event == "attached" || event == "reattached")
    {
      outcome = "success";
    }
    else if (event == "create_failed" || event == "attach_failed" || event == "reattach_failed")
    {
      outcome = "error";
    }
    else if (event == "discarded")
    {
      outcome = "invalidated";
    }
    else
    {
      return;
    }
    if (auto instruments = GetInstruments())
    {
      instruments->Lifecycle->Add(
          1, {{"component", otel::nostd::string_view{component}}, {"outcome", outcome}});
    }
  }
}}}} // namespace Azure::Messaging::EventHubs::_detail
