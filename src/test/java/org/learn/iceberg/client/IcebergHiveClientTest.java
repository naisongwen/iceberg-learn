package org.learn.iceberg.client;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.iceberg.common.DynMethods;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class IcebergHiveClientTest {
    HiveConf hiveConf;
    String hmsUri = "thrift://10.201.0.202:30470";

    @Before
    public void init() {
        hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.warehouse.dir", "s3a://bucket/minio_217/");
        hiveConf.set("metastore.catalog.default", "minio_217");
        hiveConf.set("hive.metastore.client.capability.check", "false");
        hiveConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hiveConf.set("hive.metastore.uris", hmsUri);
        hiveConf.set("fs.s3a.connection.ssl.enabled", "false");
        hiveConf.set("fs.s3a.access.key", "deepexi2022");
        hiveConf.set("fs.s3a.secret.key", "deepexi2022");
        hiveConf.set("fs.s3a.endpoint", "http://10.201.0.202:30977");
    }

    @Test
    public void testHiveClient() {
        final DynMethods.StaticMethod GET_CLIENT = DynMethods.builder("getProxy")
                .impl(RetryingMetaStoreClient.class, new Class[]{HiveConf.class, HiveMetaHookLoader.class, String.class})
                .impl(RetryingMetaStoreClient.class, new Class[]{Configuration.class, HiveMetaHookLoader.class, String.class})
                .buildStatic();

        IMetaStoreClient client = (IMetaStoreClient) GET_CLIENT.invoke(new Object[]{this.hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName()});
    }


    @Test
    public void testMetastoreClientProxy() throws MetaException {
        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(this.hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName());
        assert client != null;
    }

    @Test
    public void testDynamicProxy(){
        Map hashMap= Maps.newHashMap();
        Map proxyInstance = (Map) Proxy.newProxyInstance(
                DynamicInvocationHandler.class.getClassLoader(),
                new Class[] { Map.class },
                new DynamicInvocationHandler(hashMap));
        proxyInstance.put("hello","world");
    }

    public static class DynamicInvocationHandler implements InvocationHandler {

        private static Logger LOGGER = LoggerFactory.getLogger(
                DynamicInvocationHandler.class);

        Map map;

        public DynamicInvocationHandler(Map map){
            this.map=map;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            LOGGER.info("Invoked method:{},args:{}", method.getName(),args);
            return method.invoke(this.map,args);
        }
    }



    @Test
    public void testMetastoreClientProxy2() throws TException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        Class[] constructorArgTypes = new Class[]{Configuration.class, HiveMetaHookLoader.class,  Boolean.class};
        Object[] constructorArgs = new Object[]{this.hiveConf, (HiveMetaHookLoader) tbl -> null, true};

        MetaStoreClientHandler handler =
                new MetaStoreClientHandler(constructorArgTypes, constructorArgs,HiveMetaStoreClient.class);

        IMetaStoreClient client= (IMetaStoreClient) Proxy.newProxyInstance(
                MetaStoreClientHandler.class.getClassLoader(), HiveMetaStoreClient.class.getInterfaces(), handler);
        Database database= new DatabaseBuilder().setCatalogName("hdfs202_299").setName("test_db").create(client,hiveConf);
        List<String> list=client.getAllDatabases();
    }

    public static class MetaStoreClientHandler implements InvocationHandler {
        private final IMetaStoreClient base;

        public MetaStoreClientHandler(Class<?>[] constructorArgTypes,
                                       Object[] constructorArgs,
                                       Class<? extends IMetaStoreClient> msClientClass) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
            Constructor meth = msClientClass.getDeclaredConstructor(constructorArgTypes);
            meth.setAccessible(true);
            this.base = (IMetaStoreClient) meth.newInstance(constructorArgs);
        }
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return method.invoke(this.base, args);
        }
    }
}
