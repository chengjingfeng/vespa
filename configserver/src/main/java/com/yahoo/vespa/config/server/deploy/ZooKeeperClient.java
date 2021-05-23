// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.deploy;

import com.yahoo.component.Version;
import com.yahoo.config.application.api.ApplicationFile;
import com.yahoo.config.application.api.ApplicationMetaData;
import com.yahoo.config.application.api.ApplicationPackage;
import com.yahoo.config.application.api.DeployLogger;
import com.yahoo.config.application.api.FileRegistry;
import com.yahoo.config.application.api.UnparsedConfigDefinition;
import com.yahoo.config.model.application.provider.PreGeneratedFileRegistry;
import com.yahoo.config.provision.AllocatedHosts;
import com.yahoo.config.provision.serialization.AllocatedHostsSerializer;
import com.yahoo.io.reader.NamedReader;
import java.util.logging.Level;
import com.yahoo.path.Path;
import com.yahoo.vespa.config.ConfigDefinitionKey;
import com.yahoo.vespa.config.server.zookeeper.ConfigCurator;
import com.yahoo.vespa.config.server.zookeeper.ZKApplicationPackage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A class used for reading and writing application data to zookeeper.
 *
 * @author hmusum
 */
public class ZooKeeperClient {

    private final ConfigCurator configCurator;
    private final DeployLogger logger;
    /* This is the generation that will be used for reading and writing application data. (1 more than last deployed application) */
    private final Path rootPath;

    private static final ApplicationFile.PathFilter xmlFilter = path -> path.getName().endsWith(".xml");

    public ZooKeeperClient(ConfigCurator configCurator, DeployLogger logger, Path rootPath) {
        this.configCurator = configCurator;
        this.logger = logger;
        this.rootPath = rootPath;
    }

    /**
     * Sets up basic node structure in ZooKeeper and purges old data.
     * This is the first operation on ZK during deploy-application.
     *
     * We have retries in this method because there have been cases of stray connection loss to ZK,
     * even though the user has started the config server.
     *
     */
    void setupZooKeeper() {
        int retries = 5;
        try {
            while (retries > 0) {
                try {
                    createZooKeeperNodes();
                    break;
                } catch (RuntimeException e) {
                    logger.log(Level.FINE, "ZK init failed, retrying: " + e);
                    retries--;
                    if (retries == 0) {
                        throw e;
                    }
                    Thread.sleep(100);
                    // Not reconnecting, ZK is supposed to handle that automatically
                    // as long as the session doesn't expire. We'll see.
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to initialize vespa model writing to config server(s) " +
                                            System.getProperty("configsources") + "\n" +
                                            "Please ensure that cloudconfig_server is started on the config server node(s), " +
                                            "and check the vespa log for configserver errors. ", e);
        }
    }

    /** Sets the app id and attempts to set up zookeeper. The app id must be ordered for purge to work OK. */
    private void createZooKeeperNodes() {
        if ( ! configCurator.exists(rootPath.getAbsolute()))
            configCurator.createNode(rootPath.getAbsolute());

        for (String subPath : Arrays.asList(ConfigCurator.DEFCONFIGS_ZK_SUBPATH,
                                            ConfigCurator.USER_DEFCONFIGS_ZK_SUBPATH,
                                            ConfigCurator.USERAPP_ZK_SUBPATH,
                                            ZKApplicationPackage.fileRegistryNode)) {
            // TODO: The replaceFirst below is hackish.
            configCurator.createNode(getZooKeeperAppPath(null).getAbsolute(),
                                     subPath.replaceFirst("/", ""));
        }
    }

    /**
     * Writes def files and user config into ZK.
     *
     * @param app the application package to feed to zookeeper
     */
    void write(ApplicationPackage app) {
        try {
            writeUserDefs(app);
            writeSomeOf(app);
            writeSearchDefinitions(app);
            writeUserIncludeDirs(app, app.getUserIncludeDirs());
            write(app.getMetaData());
        } catch (Exception e) {
            throw new IllegalStateException("Unable to write vespa model to config server(s) " + System.getProperty("configsources") + "\n" +
                                            "Please ensure that cloudconfig_server is started on the config server node(s), " +
                                            "and check the vespa log for configserver errors. ", e);
        }
    }

    private void writeSearchDefinitions(ApplicationPackage app) throws IOException {
        Collection<NamedReader> sds = app.getSearchDefinitions();
        if (sds.isEmpty()) return;

        // TODO: Change to SCHEMAS_DIR after March 2020
        // TODO: When it does also check RankExpressionFile.sendTo
        Path zkPath = getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.SEARCH_DEFINITIONS_DIR);
        configCurator.createNode(zkPath.getAbsolute());
        // Ensures that ranking expressions and other files are also written
        writeDir(app.getFile(ApplicationPackage.SEARCH_DEFINITIONS_DIR), zkPath, false);
        writeDir(app.getFile(ApplicationPackage.SCHEMAS_DIR), zkPath, false);
        for (NamedReader sd : sds) {
            configCurator.putData(zkPath.getAbsolute(), sd.getName(), com.yahoo.io.IOUtils.readAll(sd.getReader()));
            sd.getReader().close();
        }
    }

    /**
     * Puts some of the application package files into ZK - see write(app).
     *
     * @param app the application package to use as input.
     * @throws java.io.IOException if not able to write to Zookeeper
     */
    private void writeSomeOf(ApplicationPackage app) throws IOException {
        // TODO: We should have a way of doing this which doesn't require repeating all the content
        writeFile(app.getFile(Path.fromString(ApplicationPackage.SERVICES)),
                  getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH));
        writeFile(app.getFile(Path.fromString(ApplicationPackage.HOSTS)),
                  getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH));
        writeFile(app.getFile(Path.fromString(ApplicationPackage.DEPLOYMENT_FILE.getName())),
                  getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH));
        writeFile(app.getFile(Path.fromString(ApplicationPackage.VALIDATION_OVERRIDES.getName())),
                  getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH));
        writeDir(app.getFile(ApplicationPackage.RULES_DIR),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.RULES_DIR),
                 (path) -> path.getName().endsWith(ApplicationPackage.RULES_NAME_SUFFIX),
                 true);
        writeDir(app.getFile(ApplicationPackage.QUERY_PROFILES_DIR),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.QUERY_PROFILES_DIR),
                 xmlFilter, true);
        writeDir(app.getFile(ApplicationPackage.PAGE_TEMPLATES_DIR),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.PAGE_TEMPLATES_DIR),
                 xmlFilter, true);
        writeDir(app.getFile(Path.fromString(ApplicationPackage.SEARCHCHAINS_DIR)),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.SEARCHCHAINS_DIR),
                 xmlFilter, true);
        writeDir(app.getFile(Path.fromString(ApplicationPackage.DOCPROCCHAINS_DIR)),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.DOCPROCCHAINS_DIR),
                 xmlFilter, true);
        writeDir(app.getFile(Path.fromString(ApplicationPackage.ROUTINGTABLES_DIR)),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.ROUTINGTABLES_DIR),
                 xmlFilter, true);
        writeDir(app.getFile(ApplicationPackage.MODELS_GENERATED_REPLICATED_DIR),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.MODELS_GENERATED_REPLICATED_DIR),
                 true);
        writeDir(app.getFile(ApplicationPackage.SECURITY_DIR),
                 getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH).append(ApplicationPackage.SECURITY_DIR),
                 true);
    }

    private void writeDir(ApplicationFile file, Path zooKeeperAppPath, boolean recurse) throws IOException {
        writeDir(file, zooKeeperAppPath, (__) -> true, recurse);
    }

    private void writeDir(ApplicationFile dir, Path path, ApplicationFile.PathFilter filenameFilter, boolean recurse) throws IOException {
        if ( ! dir.isDirectory()) return;
        for (ApplicationFile file : listFiles(dir, filenameFilter)) {
            String name = file.getPath().getName();
            if (name.startsWith(".")) continue; //.svn , .git ...
            if ("CVS".equals(name)) continue;
            if (file.isDirectory()) {
                configCurator.createNode(path.append(name).getAbsolute());
                if (recurse) {
                    writeDir(file, path.append(name), filenameFilter, recurse);
                }
            } else {
                writeFile(file, path);
            }
        }
    }

    /**
     * Like {@link ApplicationFile#listFiles(com.yahoo.config.application.api.ApplicationFile.PathFilter)}
     * with slightly different semantics: Never filter out directories.
     */
    private List<ApplicationFile> listFiles(ApplicationFile dir, ApplicationFile.PathFilter filter) {
        List<ApplicationFile> rawList = dir.listFiles();
        List<ApplicationFile> ret = new ArrayList<>();
        if (rawList != null) {
            for (ApplicationFile f : rawList) {
                if (f.isDirectory()) {
                    ret.add(f);
                } else {
                    if (filter.accept(f.getPath())) {
                        ret.add(f);
                    }
                }
            }
        }
        return ret;
    }

    private void writeFile(ApplicationFile file, Path zkPath) throws IOException {
        if ( ! file.exists()) return;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream inputStream = file.createInputStream()) {
            inputStream.transferTo(baos);
            baos.flush();
            configCurator.putData(zkPath.append(file.getPath().getName()).getAbsolute(), baos.toByteArray());
        }
    }

    private void writeUserIncludeDirs(ApplicationPackage applicationPackage, List<String> userIncludeDirs) throws IOException {
        // User defined include directories
        for (String userInclude : userIncludeDirs) {
            ApplicationFile dir = applicationPackage.getFile(Path.fromString(userInclude));
            final List<ApplicationFile> files = dir.listFiles();
            if (files == null || files.isEmpty()) {
                configCurator.createNode(getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH + "/" + userInclude).getAbsolute());
            }
            writeDir(dir,
                     getZooKeeperAppPath(ConfigCurator.USERAPP_ZK_SUBPATH + "/" + userInclude),
                     xmlFilter, true);
        }
    }

    /**
     * Feeds all user-defined .def file from the application package into ZooKeeper (both into
     * /defconfigs and /userdefconfigs
     */
    private void writeUserDefs(ApplicationPackage applicationPackage) {
        Map<ConfigDefinitionKey, UnparsedConfigDefinition> configDefs = applicationPackage.getAllExistingConfigDefs();
        for (Map.Entry<ConfigDefinitionKey, UnparsedConfigDefinition> entry : configDefs.entrySet()) {
            ConfigDefinitionKey key = entry.getKey();
            String contents = entry.getValue().getUnparsedContent();
            writeConfigDefinition(key.getName(), key.getNamespace(), getZooKeeperAppPath(ConfigCurator.USER_DEFCONFIGS_ZK_SUBPATH).getAbsolute(), contents);
            writeConfigDefinition(key.getName(), key.getNamespace(), getZooKeeperAppPath(ConfigCurator.DEFCONFIGS_ZK_SUBPATH).getAbsolute(), contents);
        }
        logger.log(Level.FINE, configDefs.size() + " user config definitions");
    }

    private void writeConfigDefinition(String name, String namespace, String path, String data) {
        configCurator.putDefData(namespace + "." + name, path, com.yahoo.text.Utf8.toBytes(data));
    }

    private void write(Version vespaVersion, FileRegistry fileRegistry) {
        String exportedRegistry = PreGeneratedFileRegistry.exportRegistry(fileRegistry);

        configCurator.putData(getZooKeeperAppPath(null).append(ZKApplicationPackage.fileRegistryNode).getAbsolute(),
                              vespaVersion.toFullString(),
                              exportedRegistry);
    }

    /**
     * Feeds application metadata to zookeeper. Used by vespamodel to create config
     * for application metadata (used by ApplicationStatusHandler)
     *
     * @param metaData The application metadata.
     */
    private void write(ApplicationMetaData metaData) {
        configCurator.putData(getZooKeeperAppPath(ConfigCurator.META_ZK_PATH).getAbsolute(), metaData.asJsonBytes());
    }

    void cleanupZooKeeper() {
        try {
            for (String subPath : Arrays.asList(
                    ConfigCurator.DEFCONFIGS_ZK_SUBPATH,
                    ConfigCurator.USER_DEFCONFIGS_ZK_SUBPATH,
                    ConfigCurator.USERAPP_ZK_SUBPATH)) {
                configCurator.deleteRecurse(getZooKeeperAppPath(null).append(subPath).getAbsolute());
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Could not clean up in zookeeper");
            //Might be called in an exception handler before re-throw, so do not throw here.
        }
    }

    /**
     * Gets a full ZK app path based on id set in Admin object
     *
     *
     * @param trailingPath trailing part of path to be appended to ZK app path
     * @return a String with the full ZK application path including trailing path, if set
     */
    private Path getZooKeeperAppPath(String trailingPath) {
        if (trailingPath != null) {
            return rootPath.append(trailingPath);
        } else {
            return rootPath;
        }
    }

    public void write(AllocatedHosts hosts) throws IOException {
        configCurator.putData(rootPath.append(ZKApplicationPackage.allocatedHostsNode).getAbsolute(),
                              AllocatedHostsSerializer.toJson(hosts));
    }

    public void write(Map<Version, FileRegistry> fileRegistryMap) {
        for (Map.Entry<Version, FileRegistry> versionFileRegistryEntry : fileRegistryMap.entrySet()) {
            write(versionFileRegistryEntry.getKey(), versionFileRegistryEntry.getValue());
        }
    }

}
