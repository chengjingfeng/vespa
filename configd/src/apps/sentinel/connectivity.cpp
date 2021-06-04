// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "connectivity.h"
#include "outward-check.h"
#include <vespa/defaults.h>
#include <vespa/log/log.h>
#include <vespa/vespalib/util/exceptions.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <thread>
#include <chrono>

LOG_SETUP(".connectivity");

using vespalib::make_string_short::fmt;
using namespace std::chrono_literals;

namespace config::sentinel {

Connectivity::Connectivity(const SentinelConfig::Connectivity & config, RpcServer &rpcServer)
  : _config(config),
    _rpcServer(rpcServer)
{
    LOG(config, "connectivity.allowedBadReversePercent = %d", _config.allowedBadReversePercent);
    LOG(config, "connectivity.allowedBadReverseCount = %d", _config.allowedBadReverseCount);
    LOG(config, "connectivity.allowedBadOutPercent = %d", _config.allowedBadOutPercent);
}

Connectivity::~Connectivity() = default;

namespace {

const char *toString(CcResult value) {
    switch (value) {
        case CcResult::UNKNOWN: return "BAD: missing result"; // very very bad
        case CcResult::REVERSE_FAIL: return "connect OK, but reverse check FAILED"; // very bad
        case CcResult::CONN_FAIL: return "failed to connect"; // bad
        case CcResult::REVERSE_UNAVAIL: return "connect OK (but reverse check unavailable)"; // unfortunate
        case CcResult::ALL_OK: return "OK: both ways connectivity verified"; // good
    }
    LOG(error, "Unknown CcResult enum value: %d", (int)value);
    LOG_ABORT("Unknown CcResult enum value");
}

std::map<std::string, std::string> specsFrom(const ModelConfig &model) {
    std::map<std::string, std::string> checkSpecs;
    for (const auto & h : model.hosts) {
        bool foundSentinelPort = false;
        for (const auto & s : h.services) {
            if (s.name == "config-sentinel") {
                for (const auto & p : s.ports) {
                    if (p.tags.find("rpc") != p.tags.npos) {
                        auto spec = fmt("tcp/%s:%d", h.name.c_str(), p.number);
                        checkSpecs[h.name] = spec;
                        foundSentinelPort = true;
                    }
                }
            }
        }
        if (! foundSentinelPort) {
            LOG(warning, "Did not find 'config-sentinel' RPC port in model for host %s [%zd services]",
                h.name.c_str(), h.services.size());
        }
    }
    return checkSpecs;
}

}

Connectivity::CheckResult
Connectivity::checkConnectivity(const ModelConfig &model) {
    CheckResult result{false, {}};
    const auto checkSpecs = specsFrom(model);
    size_t clusterSize = checkSpecs.size();
    OutwardCheckContext checkContext(clusterSize,
                                     vespa::Defaults::vespaHostname(),
                                     _rpcServer.getPort(),
                                     _rpcServer.orb());
    std::map<std::string, OutwardCheck> connectivityMap;
    for (const auto & [ hn, spec ] : checkSpecs) {
        connectivityMap.try_emplace(hn, spec, checkContext);
    }
    checkContext.latch.await();
    size_t numFailedConns = 0;
    size_t numFailedReverse = 0;
    bool allChecksOk = true;
    for (const auto & [hostname, check] : connectivityMap) {
        if (check.result() == CcResult::CONN_FAIL) ++numFailedConns;
        if (check.result() == CcResult::REVERSE_FAIL) ++numFailedReverse;
        if (check.result() == CcResult::UNKNOWN) {
            LOG(error, "Missing ConnectivityCheck result from %s", hostname.c_str());
            allChecksOk = false;
        }
    }
    if (numFailedReverse * 100ul > _config.allowedBadReversePercent * clusterSize) {
        double pct = numFailedReverse * 100.0 / clusterSize;
        LOG(warning, "%zu of %zu nodes report problems connecting to me, %.2f %% (max is %d)",
            numFailedReverse, clusterSize, pct, _config.allowedBadReversePercent);
        allChecksOk = false;
    }
    if (numFailedReverse > size_t(_config.allowedBadReverseCount)) {
        LOG(warning, "%zu of %zu nodes report problems connecting to me (max is %d)",
            numFailedReverse, clusterSize, _config.allowedBadReverseCount);
        allChecksOk = false;
    }
    if (numFailedConns * 100ul > _config.allowedBadOutPercent * clusterSize) {
        double pct = numFailedConns * 100ul / clusterSize;
        LOG(warning, "Problems connecting to %zu of %zu nodes, %.2f %% (max is %d)",
            numFailedConns, clusterSize, pct, _config.allowedBadOutPercent);
        allChecksOk = false;
    }
    for (const auto & [hostname, check] : connectivityMap) {
        std::string detail = fmt("%s -> %s", hostname.c_str(), toString(check.result()));
        result.details.push_back(detail);
    }
    result.ok = allChecksOk;
    return result;
}

}
