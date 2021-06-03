// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.flags;

import com.yahoo.component.Vtag;
import com.yahoo.vespa.defaults.Defaults;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import static com.yahoo.vespa.flags.FetchVector.Dimension.APPLICATION_ID;
import static com.yahoo.vespa.flags.FetchVector.Dimension.CLUSTER_TYPE;
import static com.yahoo.vespa.flags.FetchVector.Dimension.HOSTNAME;
import static com.yahoo.vespa.flags.FetchVector.Dimension.TENANT_ID;
import static com.yahoo.vespa.flags.FetchVector.Dimension.VESPA_VERSION;
import static com.yahoo.vespa.flags.FetchVector.Dimension.ZONE_ID;

/**
 * Definitions of feature flags.
 *
 * <p>To use feature flags, define the flag in this class as an "unbound" flag, e.g. {@link UnboundBooleanFlag}
 * or {@link UnboundStringFlag}. At the location you want to get the value of the flag, you need the following:</p>
 *
 * <ol>
 *     <li>The unbound flag</li>
 *     <li>A {@link FlagSource}. The flag source is typically available as an injectable component. Binding
 *     an unbound flag to a flag source produces a (bound) flag, e.g. {@link BooleanFlag} and {@link StringFlag}.</li>
 *     <li>If you would like your flag value to be dependent on e.g. the application ID, then 1. you should
 *     declare this in the unbound flag definition in this file (referring to
 *     {@link FetchVector.Dimension#APPLICATION_ID}), and 2. specify the application ID when retrieving the value, e.g.
 *     {@link BooleanFlag#with(FetchVector.Dimension, String)}. See {@link FetchVector} for more info.</li>
 * </ol>
 *
 * <p>Once the code is in place, you can override the flag value. This depends on the flag source, but typically
 * there is a REST API for updating the flags in the config server, which is the root of all flag sources in the zone.</p>
 *
 * @author hakonhall
 */
public class Flags {
    private static volatile TreeMap<FlagId, FlagDefinition> flags = new TreeMap<>();

    public static final UnboundStringFlag ALLOCATE_OS_REQUIREMENT = defineStringFlag(
            "allocate-os-requirement", "any",
            List.of("hakonhall"), "2021-01-26", "2021-07-26",
            "Allocations of new nodes are limited to the given host OS.  Must be one of 'rhel7', " +
            "'rhel8', or 'any'",
            "Takes effect on next (re)deployment.",
            APPLICATION_ID);

    public static final UnboundDoubleFlag DEFAULT_TERM_WISE_LIMIT = defineDoubleFlag(
            "default-term-wise-limit", 1.0,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Default limit for when to apply termwise query evaluation",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundStringFlag FEED_SEQUENCER_TYPE = defineStringFlag(
            "feed-sequencer-type", "LATENCY",
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Selects type of sequenced executor used for feeding, valid values are LATENCY, ADAPTIVE, THROUGHPUT",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundStringFlag RESPONSE_SEQUENCER_TYPE = defineStringFlag(
            "response-sequencer-type", "ADAPTIVE",
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Selects type of sequenced executor used for mbus responses, valid values are LATENCY, ADAPTIVE, THROUGHPUT",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundIntFlag RESPONSE_NUM_THREADS = defineIntFlag(
            "response-num-threads", 2,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Number of threads used for mbus responses, default is 2, negative number = numcores/4",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag SKIP_COMMUNICATIONMANAGER_THREAD = defineFeatureFlag(
            "skip-communicatiomanager-thread", false,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Should we skip the communicationmanager thread",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag SKIP_MBUS_REQUEST_THREAD = defineFeatureFlag(
            "skip-mbus-request-thread", false,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Should we skip the mbus request thread",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag SKIP_MBUS_REPLY_THREAD = defineFeatureFlag(
            "skip-mbus-reply-thread", false,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Should we skip the mbus reply thread",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag USE_THREE_PHASE_UPDATES = defineFeatureFlag(
            "use-three-phase-updates", false,
            List.of("vekterli"), "2020-12-02", "2021-08-01",
            "Whether to enable the use of three-phase updates when bucket replicas are out of sync.",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    // TODO: Remove when models referring to this are gone in all systems
    public static final UnboundBooleanFlag TENANT_IAM_ROLE = defineFeatureFlag(
            "application-iam-roles", false,
            List.of("tokle"), "2020-12-02", "2021-08-01",
            "Allow separate iam roles when provisioning/assigning hosts",
            "Takes effect immediately on new hosts, on next redeploy for applications",
            TENANT_ID);

    public static final UnboundBooleanFlag HIDE_SHARED_ROUTING_ENDPOINT = defineFeatureFlag(
            "hide-shared-routing-endpoint", false,
            List.of("tokle", "bjormel"), "2020-12-02", "2021-09-01",
            "Whether the controller should hide shared routing layer endpoint",
            "Takes effect immediately",
            APPLICATION_ID
    );

    public static final UnboundBooleanFlag USE_ASYNC_MESSAGE_HANDLING_ON_SCHEDULE = defineFeatureFlag(
            "async-message-handling-on-schedule", false,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "Optionally deliver async messages in own thread",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundDoubleFlag FEED_CONCURRENCY = defineDoubleFlag(
            "feed-concurrency", 0.5,
            List.of("baldersheim"), "2020-12-02", "2022-01-01",
            "How much concurrency should be allowed for feed",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag GROUP_SUSPENSION = defineFeatureFlag(
            "group-suspension", true,
            List.of("hakon"), "2021-01-22", "2021-06-22",
            "Allow all content nodes in a hierarchical group to suspend at the same time",
            "Takes effect on the next suspension request to the Orchestrator.",
            APPLICATION_ID);

    public static final UnboundBooleanFlag ENCRYPT_DISK = defineFeatureFlag(
            "encrypt-disk", false,
            List.of("hakonhall"), "2021-05-05", "2021-06-05",
            "Allow migrating an unencrypted data partition to being encrypted.",
            "Takes effect on next host-admin tick.");

    public static final UnboundBooleanFlag ENCRYPT_DIRTY_DISK = defineFeatureFlag(
            "encrypt-dirty-disk", false,
            List.of("hakonhall"), "2021-05-14", "2021-06-05",
            "Allow migrating an unencrypted data partition to being encrypted when provisioned or dirty.",
            "Takes effect on next host-admin tick.");

    public static final UnboundBooleanFlag ENABLE_FEED_BLOCK_IN_DISTRIBUTOR = defineFeatureFlag(
            "enable-feed-block-in-distributor", true,
            List.of("geirst"), "2021-01-27", "2021-07-01",
            "Enables blocking of feed in the distributor if resource usage is above limit on at least one content node",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundIntFlag METRICS_PROXY_MAX_HEAP_SIZE_IN_MB = defineIntFlag(
            "metrics-proxy-max-heap-size-in-mb", 256,
            List.of("hmusum"), "2021-03-01", "2021-06-15",
            "JVM max heap size for metrics proxy in Mb",
            "Takes effect when restarting metrics proxy",
            CLUSTER_TYPE);

    public static final UnboundStringFlag DEDICATED_CLUSTER_CONTROLLER_FLAVOR = defineStringFlag(
            "dedicated-cluster-controller-flavor", "", List.of("jonmv"), "2021-02-25", "2021-08-25",
            "Flavor as <vpu>-<memgb>-<diskgb> to use for dedicated cluster controller nodes",
            "Takes effect immediately, for subsequent provisioning",
            APPLICATION_ID);

    public static final UnboundListFlag<String> ALLOWED_ATHENZ_PROXY_IDENTITIES = defineListFlag(
            "allowed-athenz-proxy-identities", List.of(), String.class,
            List.of("bjorncs", "tokle"), "2021-02-10", "2021-08-01",
            "Allowed Athenz proxy identities",
            "takes effect at redeployment");

    public static final UnboundBooleanFlag GENERATE_NON_MTLS_ENDPOINT = defineFeatureFlag(
            "generate-non-mtls-endpoint", true,
            List.of("tokle"), "2021-02-18", "2021-10-01",
            "Whether to generate the non-mtls endpoint",
            "Takes effect on next internal redeployment",
            APPLICATION_ID);

    public static final UnboundIntFlag MAX_ACTIVATION_INHIBITED_OUT_OF_SYNC_GROUPS = defineIntFlag(
            "max-activation-inhibited-out-of-sync-groups", 0,
            List.of("vekterli"), "2021-02-19", "2021-07-01",
            "Allows replicas in up to N content groups to not be activated " +
            "for query visibility if they are out of sync with a majority of other replicas",
            "Takes effect at redeployment",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag ENABLE_CUSTOM_ACL_MAPPING = defineFeatureFlag(
            "enable-custom-acl-mapping", false,
            List.of("mortent","bjorncs"), "2021-04-13", "2021-08-01",
            "Whether access control filters should read acl request mapping from handler or use default",
            "Takes effect at redeployment",
            APPLICATION_ID);

    public static final UnboundIntFlag NUM_DISTRIBUTOR_STRIPES = defineIntFlag(
            "num-distributor-stripes", 0,
            List.of("geirst", "vekterli"), "2021-04-20", "2021-07-01",
            "Specifies the number of stripes used by the distributor. When 0, legacy single stripe behavior is used.",
            "Takes effect after distributor restart",
            ZONE_ID, APPLICATION_ID);

    public static final UnboundBooleanFlag USE_EXTERNAL_RANK_EXPRESSION = defineFeatureFlag(
            "use-external-rank-expression", false,
            List.of("baldersheim"), "2021-05-24", "2021-07-01",
            "Whether to use distributed external rank expression or inline in rankproperties",
            "Takes effect on next internal redeployment",
            APPLICATION_ID);

    public static final UnboundBooleanFlag DISTRIBUTE_EXTERNAL_RANK_EXPRESSION = defineFeatureFlag(
            "distribute-external-rank-expression", false,
            List.of("baldersheim"), "2021-05-27", "2021-07-01",
            "Whether to use distributed external rank expression files by filedistribution",
            "Takes effect on next internal redeployment",
            APPLICATION_ID);

    public static final UnboundBooleanFlag ENABLE_ROUTING_CORE_DUMP = defineFeatureFlag(
            "enable-routing-core-dumps", false,
            List.of("tokle"), "2021-04-16", "2021-08-01",
            "Whether to enable core dumps for routing layer",
            "Takes effect on next host-admin tick",
            HOSTNAME);

    public static final UnboundBooleanFlag CFG_DEPLOY_MULTIPART = defineFeatureFlag(
            "cfg-deploy-multipart", false,
            List.of("tokle"), "2021-05-19", "2021-08-01",
            "Whether to deploy applications using multipart form data (instead of url params)",
            "Takes effect immediately",
            APPLICATION_ID);

    public static final UnboundBooleanFlag VESPA_APP_DOMAIN_IN_CERTIFICATE = defineFeatureFlag(
            "new-domain-in-certificate", false,
            List.of("mpolden"), "2021-05-25", "2021-09-01",
            "Whether to include the vespa-app.cloud names in certificate requests",
            "Takes effect on next deployment through controller",
            APPLICATION_ID);

    public static final UnboundIntFlag MAX_ENCRYPTING_HOSTS = defineIntFlag(
            "max-encrypting-hosts", 0,
            List.of("mpolden", "hakonhall"), "2021-05-27", "2021-10-01",
            "The maximum number of hosts allowed to encrypt their disk concurrently",
            "Takes effect on next run of HostEncrypter, but any currently encrypting hosts will not be cancelled when reducing the limit");

    public static final UnboundBooleanFlag REQUIRE_CONNECTIVITY_CHECK = defineFeatureFlag(
            "require-connectivity-check", false,
            List.of("arnej"), "2021-06-03", "2021-09-01",
            "Require that config-sentinel connectivity check passes with good quality before starting services",
            "Takes effect on next restart");

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static UnboundBooleanFlag defineFeatureFlag(String flagId, boolean defaultValue, List<String> owners,
                                                       String createdAt, String expiresAt, String description,
                                                       String modificationEffect, FetchVector.Dimension... dimensions) {
        return define(UnboundBooleanFlag::new, flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static UnboundStringFlag defineStringFlag(String flagId, String defaultValue, List<String> owners,
                                                     String createdAt, String expiresAt, String description,
                                                     String modificationEffect, FetchVector.Dimension... dimensions) {
        return define(UnboundStringFlag::new, flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static UnboundIntFlag defineIntFlag(String flagId, int defaultValue, List<String> owners,
                                               String createdAt, String expiresAt, String description,
                                               String modificationEffect, FetchVector.Dimension... dimensions) {
        return define(UnboundIntFlag::new, flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static UnboundLongFlag defineLongFlag(String flagId, long defaultValue, List<String> owners,
                                                 String createdAt, String expiresAt, String description,
                                                 String modificationEffect, FetchVector.Dimension... dimensions) {
        return define(UnboundLongFlag::new, flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static UnboundDoubleFlag defineDoubleFlag(String flagId, double defaultValue, List<String> owners,
                                                     String createdAt, String expiresAt, String description,
                                                     String modificationEffect, FetchVector.Dimension... dimensions) {
        return define(UnboundDoubleFlag::new, flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static <T> UnboundJacksonFlag<T> defineJacksonFlag(String flagId, T defaultValue, Class<T> jacksonClass, List<String> owners,
                                                              String createdAt, String expiresAt, String description,
                                                              String modificationEffect, FetchVector.Dimension... dimensions) {
        return define((id2, defaultValue2, vector2) -> new UnboundJacksonFlag<>(id2, defaultValue2, vector2, jacksonClass),
                flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    /** WARNING: public for testing: All flags should be defined in {@link Flags}. */
    public static <T> UnboundListFlag<T> defineListFlag(String flagId, List<T> defaultValue, Class<T> elementClass,
                                                        List<String> owners, String createdAt, String expiresAt,
                                                        String description, String modificationEffect, FetchVector.Dimension... dimensions) {
        return define((fid, dval, fvec) -> new UnboundListFlag<>(fid, dval, elementClass, fvec),
                flagId, defaultValue, owners, createdAt, expiresAt, description, modificationEffect, dimensions);
    }

    @FunctionalInterface
    private interface TypedUnboundFlagFactory<T, U extends UnboundFlag<?, ?, ?>> {
        U create(FlagId id, T defaultVale, FetchVector defaultFetchVector);
    }

    /**
     * Defines a Flag.
     *
     * @param factory            Factory for creating unbound flag of type U
     * @param flagId             The globally unique FlagId.
     * @param defaultValue       The default value if none is present after resolution.
     * @param description        Description of how the flag is used.
     * @param modificationEffect What is required for the flag to take effect? A restart of process? immediately? etc.
     * @param dimensions         What dimensions will be set in the {@link FetchVector} when fetching
     *                           the flag value in
     *                           {@link FlagSource#fetch(FlagId, FetchVector) FlagSource::fetch}.
     *                           For instance, if APPLICATION is one of the dimensions here, you should make sure
     *                           APPLICATION is set to the ApplicationId in the fetch vector when fetching the RawFlag
     *                           from the FlagSource.
     * @param <T>                The boxed type of the flag value, e.g. Boolean for flags guarding features.
     * @param <U>                The type of the unbound flag, e.g. UnboundBooleanFlag.
     * @return An unbound flag with {@link FetchVector.Dimension#HOSTNAME HOSTNAME} and
     *         {@link FetchVector.Dimension#VESPA_VERSION VESPA_VERSION} already set. The ZONE environment
     *         is typically implicit.
     */
    private static <T, U extends UnboundFlag<?, ?, ?>> U define(TypedUnboundFlagFactory<T, U> factory,
                                                                String flagId,
                                                                T defaultValue,
                                                                List<String> owners,
                                                                String createdAt,
                                                                String expiresAt,
                                                                String description,
                                                                String modificationEffect,
                                                                FetchVector.Dimension[] dimensions) {
        FlagId id = new FlagId(flagId);
        FetchVector vector = new FetchVector()
                .with(HOSTNAME, Defaults.getDefaults().vespaHostname())
                // Warning: In unit tests and outside official Vespa releases, the currentVersion is e.g. 7.0.0
                // (determined by the current major version). Consider not setting VESPA_VERSION if minor = micro = 0.
                .with(VESPA_VERSION, Vtag.currentVersion.toFullString());
        U unboundFlag = factory.create(id, defaultValue, vector);
        FlagDefinition definition = new FlagDefinition(
                unboundFlag, owners, parseDate(createdAt), parseDate(expiresAt), description, modificationEffect, dimensions);
        flags.put(id, definition);
        return unboundFlag;
    }

    private static Instant parseDate(String rawDate) {
        return DateTimeFormatter.ISO_DATE.parse(rawDate, LocalDate::from).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    public static List<FlagDefinition> getAllFlags() {
        return List.copyOf(flags.values());
    }

    public static Optional<FlagDefinition> getFlag(FlagId flagId) {
        return Optional.ofNullable(flags.get(flagId));
    }

    /**
     * Allows the statically defined flags to be controlled in a test.
     *
     * <p>Returns a Replacer instance to be used with e.g. a try-with-resources block. Within the block,
     * the flags starts out as cleared. Flags can be defined, etc. When leaving the block, the flags from
     * before the block is reinserted.
     *
     * <p>NOT thread-safe. Tests using this cannot run in parallel.
     */
    public static Replacer clearFlagsForTesting() {
        return new Replacer();
    }

    public static class Replacer implements AutoCloseable {
        private static volatile boolean flagsCleared = false;

        private final TreeMap<FlagId, FlagDefinition> savedFlags;

        private Replacer() {
            verifyAndSetFlagsCleared(true);
            this.savedFlags = Flags.flags;
            Flags.flags = new TreeMap<>();
        }

        @Override
        public void close() {
            verifyAndSetFlagsCleared(false);
            Flags.flags = savedFlags;
        }

        /**
         * Used to implement a simple verification that Replacer is not used by multiple threads.
         * For instance two different tests running in parallel cannot both use Replacer.
         */
        private static void verifyAndSetFlagsCleared(boolean newValue) {
            if (flagsCleared == newValue) {
                throw new IllegalStateException("clearFlagsForTesting called while already cleared - running tests in parallell!?");
            }
            flagsCleared = newValue;
        }
    }
}
