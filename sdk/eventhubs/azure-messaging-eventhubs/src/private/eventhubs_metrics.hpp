// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace Azure { namespace Messaging { namespace EventHubs { namespace _detail {

  // One scope per public operation, including preparation and all retries.
  class OperationMetrics final {
#if defined(AZURE_EVENTHUBS_ENABLE_METRICS)
    char const* m_operation;
    std::string const& m_hub;
    std::string const& m_host;
    std::chrono::steady_clock::time_point m_start{std::chrono::steady_clock::now()};
    std::uint64_t m_events{};
    char const* m_outcome{"error"};

  public:
    OperationMetrics(char const* operation, std::string const& hub, std::string const& host)
        : m_operation(operation), m_hub(hub), m_host(host)
    {
    }
    ~OperationMetrics();
    void Complete(std::uint64_t events, bool cancelled = false)
    {
      m_events = events;
      m_outcome = cancelled ? "cancelled" : "success";
    }
#else
  public:
    OperationMetrics(char const*, std::string const&, std::string const&) {}
    void Complete(std::uint64_t, bool = false) {}
#endif
    OperationMetrics(OperationMetrics const&) = delete;
    OperationMetrics& operator=(OperationMetrics const&) = delete;
  };

#if defined(AZURE_EVENTHUBS_ENABLE_METRICS)
  void RecordLifecycleMetric(std::string const& component, std::string const& event);
#else
  inline void RecordLifecycleMetric(std::string const&, std::string const&) {}
#endif

}}}} // namespace Azure::Messaging::EventHubs::_detail
