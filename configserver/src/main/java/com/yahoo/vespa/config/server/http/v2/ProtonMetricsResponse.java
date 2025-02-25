// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.http.v2;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.restapi.SlimeJsonResponse;
import com.yahoo.slime.Cursor;
import com.yahoo.vespa.config.server.metrics.ProtonMetricsAggregator;
import java.util.Map;

/**
 * @author akvalsvik
 */
public class ProtonMetricsResponse extends SlimeJsonResponse {

    public ProtonMetricsResponse(ApplicationId applicationId, Map<String, ProtonMetricsAggregator> aggregatedProtonMetrics) {
        Cursor application = slime.setObject();
        application.setString("applicationId", applicationId.serializedForm());

        Cursor clusters = application.setArray("clusters");

        for (var entry : aggregatedProtonMetrics.entrySet()) {
            Cursor cluster = clusters.addObject();
            cluster.setString("clusterId", entry.getKey());

            ProtonMetricsAggregator aggregator = entry.getValue();
            Cursor metrics = cluster.setObject("metrics");
            metrics.setDouble("documentsActiveCount", aggregator.aggregateDocumentActiveCount());
            metrics.setDouble("documentsReadyCount", aggregator.aggregateDocumentReadyCount());
            metrics.setDouble("documentsTotalCount", aggregator.aggregateDocumentTotalCount());
            metrics.setDouble("documentDiskUsage", aggregator.aggregateDocumentDiskUsage());
            metrics.setDouble("resourceDiskUsageAverage", aggregator.aggregateResourceDiskUsageAverage());
            metrics.setDouble("resourceMemoryUsageAverage", aggregator.aggregateResourceMemoryUsageAverage());
        }
    }
}
