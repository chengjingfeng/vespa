// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "cmdq.h"
#include "config-owner.h"
#include "metrics.h"
#include "rpcserver.h"
#include "state-api.h"
#include <vespa/vespalib/net/state_server.h>

namespace config::sentinel {

/**
 * Environment for config sentinel, with config
 * subscription, rpc server, state server, and
 * metrics.
 **/
class Env {
public:
    Env();
    ~Env();

    ConfigOwner &configOwner() { return _cfgOwner; }
    CommandQueue &commandQueue() { return _rpcCommandQueue; }
    StartMetrics &metrics() { return _startMetrics; }

    static constexpr int maxRetryLoops = 5;
    static constexpr int maxRetriesInsideLoop = 10;

    void boot(const std::string &configId);
    void rpcPort(int portnum);
    void statePort(int portnum);

    void notifyConfigUpdated();
private:
    void respondAsEmpty();
    bool waitForConnectivity(int outerRetry);
    ConfigOwner _cfgOwner;
    CommandQueue _rpcCommandQueue;
    std::unique_ptr<RpcServer> _rpcServer;
    StateApi _stateApi;
    StartMetrics _startMetrics;
    std::unique_ptr<vespalib::StateServer> _stateServer;
    int _statePort;
};

}
