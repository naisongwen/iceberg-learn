package org.learn.datalake.thrift;

import jodd.util.PropertiesUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HiveMetaStoreServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaStoreServer.class.getName());

    private File hiveLocalDir;
    private HiveConf hiveConf;
    private HiveClientPool clientPool;

    private String getDerbyPath() {
        File metastoreDB = new File(hiveLocalDir, "metastore_db");
        return metastoreDB.getPath();
    }

    private void dumpHiveConf(HiveConf conf, int port) throws IOException {
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
        // in Hive3, setting this as a system prop ensures that it will be picked up whenever a new HiveConf is created
        System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
        // Set another new directory which is different with the hive metastore's warehouse path.
        conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + this.hiveLocalDir.getAbsolutePath());
        System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + this.hiveLocalDir.getAbsolutePath());

        conf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
        conf.set(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
        conf.set("iceberg.hive.client-pool-size", "2");

        Properties properties = new Properties();
        properties.put(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
        properties.put(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file://" + this.hiveLocalDir.getAbsolutePath());
        PropertiesUtil.writeToFile(properties, new File("hive.properties"));
//        File hiveSiteXML = new File(this.hiveLocalDir, "hive-site.xml");
//        try (FileOutputStream fos = new FileOutputStream(hiveSiteXML)) {
//            Configuration newConf = new Configuration(hiveConf);
//            newConf.writeXml(fos);
//        }
    }

    private TServer newThriftServer(int poolSize, HiveConf conf) throws Exception {
        HiveConf serverConf = new HiveConf(conf);
        serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:" + getDerbyPath() + ";create=true");
        IHMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", serverConf);
        IHMSHandler handler = RetryingHMSHandler.getProxy(serverConf, baseHandler, false);

        TServerSocket socket = new TServerSocket(0);
        int port = socket.getServerSocket().getLocalPort();
        dumpHiveConf(conf, port);
        this.hiveConf = conf;
        LOGGER.info("start thrift server on port:%d", port);
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
                .processor(new TSetIpAddressProcessor<>(handler))
                .transportFactory(new TTransportFactory())
                .protocolFactory(new TBinaryProtocol.Factory())
                .minWorkerThreads(poolSize)
                .maxWorkerThreads(poolSize);

        return new TThreadPoolServer(args);
    }

    private void setupMetastoreDB(String dbURL) throws SQLException, IOException {
        Connection connection = DriverManager.getConnection(dbURL);
        ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("hive-schema-3.1.0.derby.sql");
        try (Reader reader = new InputStreamReader(inputStream)) {
            scriptRunner.runScript(reader);
        }
    }

    public void start(HiveConf hiveConf) throws Exception {
//        this.hiveLocalDir = createTempDirectory("warehouse").toFile();
        File warehouse = new File("warehouse");
        if (warehouse.exists())
            FileUtils.deleteDirectory(warehouse);
        this.hiveLocalDir = Files.createDirectory(Paths.get("warehouse")).toFile();
        File derbyLogFile = new File(hiveLocalDir, "derby.log");
        System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
        setupMetastoreDB("jdbc:derby:" + getDerbyPath() + ";create=true");

        this.clientPool = new HiveClientPool(3, hiveConf);
        TServer tServer = newThriftServer(3, hiveConf);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> tServer.serve());
    }

    public void start() throws Exception {
        start(new HiveConf(new Configuration(), HiveMetaStoreServer.class));
    }

    public Table getTable(String dbName, String tableName) throws TException, InterruptedException {
        return clientPool.run(client -> client.getTable(dbName, tableName));
    }

    public Table getTable(TableIdentifier identifier) throws TException, InterruptedException {
        return getTable(identifier.namespace().toString(), identifier.name());
    }

    public static void main(String[] args) throws Exception {
        HiveMetaStoreServer hiveMetaServer = new HiveMetaStoreServer();
        hiveMetaServer.start();
        //hiveMetaServer.getTable("default","table");
    }
}
