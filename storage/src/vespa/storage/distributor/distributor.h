// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "bucket_spaces_stats_provider.h"
#include "bucketdbupdater.h"
#include "distributor_component.h"
#include "distributor_host_info_reporter.h"
#include "distributor_interface.h"
#include "distributor_stripe_interface.h"
#include "externaloperationhandler.h"
#include "idealstatemanager.h"
#include "min_replica_provider.h"
#include "pendingmessagetracker.h"
#include "statusreporterdelegate.h"
#include "stripe_bucket_db_updater.h" // TODO this is temporary
#include "stripe_host_info_notifier.h"
#include <vespa/config/config.h>
#include <vespa/storage/common/distributorcomponent.h>
#include <vespa/storage/common/doneinitializehandler.h>
#include <vespa/storage/common/messagesender.h>
#include <vespa/storage/distributor/bucketdb/bucketdbmetricupdater.h>
#include <vespa/storage/distributor/maintenance/maintenancescheduler.h>
#include <vespa/storageapi/message/state.h>
#include <vespa/storageframework/generic/metric/metricupdatehook.h>
#include <vespa/storageframework/generic/thread/tickingthread.h>
#include <chrono>
#include <queue>
#include <unordered_map>

namespace storage {
    struct DoneInitializeHandler;
    class HostInfo;
    class NodeIdentity;
}

namespace storage::distributor {

class BlockingOperationStarter;
class BucketPriorityDatabase;
class BucketDBUpdater;
class DistributorBucketSpaceRepo;
class DistributorStatus;
class DistributorStripe;
class DistributorStripePool;
class StripeAccessor;
class OperationSequencer;
class OwnershipTransferSafeTimePointCalculator;
class SimpleMaintenanceScanner;
class ThrottlingOperationStarter;

class Distributor final
    : public StorageLink,
      public DistributorInterface,
      public StatusDelegator,
      public framework::StatusReporter,
      public framework::TickingThread,
      public MinReplicaProvider,
      public BucketSpacesStatsProvider,
      public StripeHostInfoNotifier
{
public:
    Distributor(DistributorComponentRegister&,
                const NodeIdentity& node_identity,
                framework::TickingThreadPool&,
                DistributorStripePool& stripe_pool,
                DoneInitializeHandler&,
                uint32_t num_distributor_stripes,
                HostInfo& hostInfoReporterRegistrar,
                ChainedMessageSender* = nullptr);

    ~Distributor() override;

    void onOpen() override;
    void onClose() override;
    bool onDown(const std::shared_ptr<api::StorageMessage>&) override;
    void sendUp(const std::shared_ptr<api::StorageMessage>&) override;
    void sendDown(const std::shared_ptr<api::StorageMessage>&) override;

    DistributorMetricSet& getMetrics() { return *_metrics; }

    // Implements DistributorInterface and DistributorMessageSender.
    DistributorMetricSet& metrics() override { return getMetrics(); }
    const DistributorConfiguration& config() const override;

    void sendCommand(const std::shared_ptr<api::StorageCommand>& cmd) override;
    void sendReply(const std::shared_ptr<api::StorageReply>& reply) override;
    int getDistributorIndex() const override { return _component.node_index(); }
    const ClusterContext& cluster_context() const override { return _component.cluster_context(); }

    void storageDistributionChanged() override;

    bool handleReply(const std::shared_ptr<api::StorageReply>& reply);

    // StatusReporter implementation
    vespalib::string getReportContentType(const framework::HttpUrlPath&) const override;
    bool reportStatus(std::ostream&, const framework::HttpUrlPath&) const override;

    bool handleStatusRequest(const DelegatedStatusRequest& request) const override;

    virtual framework::ThreadWaitInfo doCriticalTick(framework::ThreadIndex) override;
    virtual framework::ThreadWaitInfo doNonCriticalTick(framework::ThreadIndex) override;

    // Called by DistributorStripe threads when they want to notify the cluster controller of changed stats.
    // Thread safe.
    void notify_stripe_wants_to_send_host_info(uint16_t stripe_index) override;

    class MetricUpdateHook : public framework::MetricUpdateHook
    {
    public:
        MetricUpdateHook(Distributor& self)
            : _self(self)
        {
        }

        void updateMetrics(const MetricLockGuard &) override {
            _self.propagateInternalScanMetricsToExternal();
        }

    private:
        Distributor& _self;
    };

private:
    friend struct DistributorTest;
    friend class BucketDBUpdaterTest;
    friend class DistributorTestUtil;
    friend class MetricUpdateHook;

    // TODO STRIPE remove
    DistributorStripe& first_stripe() noexcept;
    const DistributorStripe& first_stripe() const noexcept;

    void setNodeStateUp();
    bool handleMessage(const std::shared_ptr<api::StorageMessage>& msg);

    /**
     * Enables a new cluster state. Used by tests to bypass BucketDBUpdater.
     */
    void enableClusterStateBundle(const lib::ClusterStateBundle& clusterStateBundle);

    // Accessors used by tests
    std::string getActiveIdealStateOperations() const;
    const lib::ClusterStateBundle& getClusterStateBundle() const;
    const DistributorConfiguration& getConfig() const;
    bool isInRecoveryMode() const noexcept;
    PendingMessageTracker& getPendingMessageTracker();
    const PendingMessageTracker& getPendingMessageTracker() const;
    DistributorBucketSpaceRepo& getBucketSpaceRepo() noexcept;
    const DistributorBucketSpaceRepo& getBucketSpaceRepo() const noexcept;
    DistributorBucketSpaceRepo& getReadOnlyBucketSpaceRepo() noexcept;
    const DistributorBucketSpaceRepo& getReadyOnlyBucketSpaceRepo() const noexcept;
    storage::distributor::DistributorStripeComponent& distributor_component() noexcept;
    std::chrono::steady_clock::duration db_memory_sample_interval() const noexcept;

    StripeBucketDBUpdater& bucket_db_updater();
    const StripeBucketDBUpdater& bucket_db_updater() const;
    IdealStateManager& ideal_state_manager();
    const IdealStateManager& ideal_state_manager() const;
    ExternalOperationHandler& external_operation_handler();
    const ExternalOperationHandler& external_operation_handler() const;
    BucketDBMetricUpdater& bucket_db_metric_updater() const noexcept;

    /**
     * Return a copy of the latest min replica data, see MinReplicaProvider.
     */
    std::unordered_map<uint16_t, uint32_t> getMinReplica() const override;

    PerNodeBucketSpacesStats getBucketSpacesStats() const override;
    SimpleMaintenanceScanner::PendingMaintenanceStats pending_maintenance_stats() const;

    /**
     * Atomically publish internal metrics to external ideal state metrics.
     * Takes metric lock.
     */
    void propagateInternalScanMetricsToExternal();
    void scanAllBuckets();
    void enableNextConfig();
    void fetch_status_requests();
    void handle_status_requests();
    void signal_work_was_done();
    void enableNextDistribution();
    void propagateDefaultDistribution(std::shared_ptr<const lib::Distribution>);

    void dispatch_to_main_distributor_thread_queue(const std::shared_ptr<api::StorageMessage>& msg);
    void fetch_external_messages();
    void process_fetched_external_messages();
    void send_host_info_if_appropriate();
    // Precondition: _stripe_scan_notify_mutex is held
    [[nodiscard]] bool may_send_host_info_on_behalf_of_stripes(std::lock_guard<std::mutex>& held_lock) noexcept;

    struct StripeScanStats {
        bool wants_to_send_host_info = false;
        bool has_reported_in_at_least_once = false;
    };

    using MessageQueue = std::vector<std::shared_ptr<api::StorageMessage>>;

    DistributorComponentRegister&         _comp_reg;
    std::shared_ptr<DistributorMetricSet> _metrics;
    ChainedMessageSender*                 _messageSender;
    const bool                            _use_legacy_mode;
    // TODO STRIPE multiple stripes...! This is for proof of concept of wiring.
    uint8_t                               _n_stripe_bits;
    std::unique_ptr<DistributorStripe>    _stripe;
    DistributorStripePool&                _stripe_pool;
    std::vector<std::unique_ptr<DistributorStripe>> _stripes;
    std::unique_ptr<StripeAccessor>      _stripe_accessor;
    MessageQueue                         _message_queue; // Queue for top-level ops
    MessageQueue                         _fetched_messages;
    distributor::DistributorComponent    _component;
    std::shared_ptr<const DistributorConfiguration> _total_config;
    std::unique_ptr<BucketDBUpdater>     _bucket_db_updater;
    StatusReporterDelegate               _distributorStatusDelegate;
    std::unique_ptr<StatusReporterDelegate> _bucket_db_status_delegate;
    framework::TickingThreadPool&        _threadPool;
    mutable std::vector<std::shared_ptr<DistributorStatus>> _status_to_do;
    mutable std::vector<std::shared_ptr<DistributorStatus>> _fetched_status_requests;
    mutable std::mutex                   _stripe_scan_notify_mutex;
    std::vector<StripeScanStats>         _stripe_scan_stats; // Indices are 1-1 with _stripes entries
    std::chrono::steady_clock::time_point _last_host_info_send_time;
    std::chrono::milliseconds            _host_info_send_delay;
    framework::ThreadWaitInfo            _tickResult;
    MetricUpdateHook                     _metricUpdateHook;
    DistributorHostInfoReporter          _hostInfoReporter;

    std::shared_ptr<lib::Distribution>   _distribution;
    std::shared_ptr<lib::Distribution>   _next_distribution;

    uint64_t                             _current_internal_config_generation;
};

}
