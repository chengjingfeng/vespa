// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.google.common.collect.Sets;
import com.yahoo.jdisc.Metric;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancer;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancer.State;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancerId;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancerService;
import com.yahoo.vespa.hosted.provision.lb.LoadBalancerSpec;
import com.yahoo.vespa.hosted.provision.persistence.CuratorDatabaseClient;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Periodically expire load balancers and de-provision inactive ones.
 *
 * Load balancers expire from the following states:
 *
 * {@link LoadBalancer.State#inactive}: An application is removed and load balancers are deactivated.
 * {@link LoadBalancer.State#reserved}: An prepared application is never successfully activated, thus never activating
 *                                      any prepared load balancers.
 *
 * @author mpolden
 */
public class LoadBalancerExpirer extends NodeRepositoryMaintainer {

    private static final Logger LOG = Logger.getLogger(LoadBalancerExpirer.class.getName());

    private static final Duration reservedExpiry = Duration.ofHours(1);
    private static final Duration inactiveExpiry = Duration.ofHours(1);

    private final LoadBalancerService service;
    private final CuratorDatabaseClient db;

    LoadBalancerExpirer(NodeRepository nodeRepository, Duration interval, LoadBalancerService service, Metric metric) {
        super(nodeRepository, interval, metric);
        this.service = Objects.requireNonNull(service, "service must be non-null");
        this.db = nodeRepository.database();
    }

    @Override
    protected boolean maintain() {
        expireReserved();
        return removeInactive() & pruneReals();
    }

    /** Move reserved load balancer that have expired to inactive */
    private void expireReserved() {
        Instant now = nodeRepository().clock().instant();
        Instant expiry = now.minus(reservedExpiry);
        patchLoadBalancers(lb -> lb.state() == State.reserved && lb.changedAt().isBefore(expiry),
                           lb -> db.writeLoadBalancer(lb.with(State.inactive, now)));
    }

    /** Deprovision inactive load balancers that have expired */
    private boolean removeInactive() {
        var failed = new ArrayList<LoadBalancerId>();
        var lastException = new AtomicReference<Exception>();
        var expiry = nodeRepository().clock().instant().minus(inactiveExpiry);
        patchLoadBalancers(lb -> lb.state() == State.inactive &&
                                 lb.changedAt().isBefore(expiry) &&
                                 allocatedNodes(lb.id()).isEmpty(), lb -> {
            try {
                log.log(Level.INFO, () -> "Removing expired inactive load balancer " + lb.id());
                service.remove(lb.id().application(), lb.id().cluster());
                db.removeLoadBalancer(lb.id());
            } catch (Exception e){
                failed.add(lb.id());
                lastException.set(e);
            }
        });
        if (!failed.isEmpty()) {
            log.log(Level.WARNING, lastException.get(), () -> String.format("Failed to remove %d load balancers: %s, retrying in %s",
                                                                            failed.size(),
                                                                            failed.stream()
                                                                                  .map(LoadBalancerId::serializedForm)
                                                                                  .collect(Collectors.joining(", ")),
                                                                            interval()));
        }
        return lastException.get() == null;
    }

    /** Remove reals from inactive load balancers */
    private boolean pruneReals() {
        var failed = new ArrayList<LoadBalancerId>();
        var lastException = new AtomicReference<Exception>();
        patchLoadBalancers(lb -> lb.state() == State.inactive, lb -> {
            if (lb.instance().isEmpty()) return;
            var allocatedNodes = allocatedNodes(lb.id()).stream().map(Node::hostname).collect(Collectors.toSet());
            var reals = new LinkedHashSet<>(lb.instance().get().reals());
            // Remove any real no longer allocated to this application
            reals.removeIf(real -> !allocatedNodes.contains(real.hostname().value()));
            if (reals.equals(lb.instance().get().reals())) return; // Nothing to remove
            try {
                LOG.log(Level.INFO, () -> "Removing reals from inactive load balancer " + lb.id() + ": " + Sets.difference(lb.instance().get().reals(), reals));
                service.create(new LoadBalancerSpec(lb.id().application(), lb.id().cluster(), reals), true);
                db.writeLoadBalancer(lb.with(lb.instance().map(instance -> instance.withReals(reals))));
            } catch (Exception e) {
                failed.add(lb.id());
                lastException.set(e);
            }
        });
        if (!failed.isEmpty()) {
            log.log(Level.WARNING, String.format("Failed to remove reals from %d load balancers: %s, retrying in %s",
                                                 failed.size(),
                                                 failed.stream()
                                                       .map(LoadBalancerId::serializedForm)
                                                       .collect(Collectors.joining(", ")),
                                                 interval()),
                    lastException.get());
        }
        return lastException.get() == null;
    }

    /** Patch load balancers matching given filter, while holding lock */
    private void patchLoadBalancers(Predicate<LoadBalancer> filter, Consumer<LoadBalancer> patcher) {
        for (var id : db.readLoadBalancerIds()) {
            Optional<LoadBalancer> loadBalancer = db.readLoadBalancer(id);
            if (loadBalancer.isEmpty() || !filter.test(loadBalancer.get())) continue;
            try (var lock = db.lock(id.application(), Duration.ofSeconds(1))) {
                loadBalancer = db.readLoadBalancer(id);
                if (loadBalancer.isEmpty() || !filter.test(loadBalancer.get())) continue;
                patcher.accept(loadBalancer.get());
            }
        }
    }

    private List<Node> allocatedNodes(LoadBalancerId loadBalancer) {
        return nodeRepository().nodes().list().owner(loadBalancer.application()).cluster(loadBalancer.cluster()).asList();
    }

}
