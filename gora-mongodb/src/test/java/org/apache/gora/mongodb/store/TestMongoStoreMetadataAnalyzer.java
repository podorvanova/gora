package org.apache.gora.mongodb.store;

import com.mongodb.ServerAddress;
import org.apache.gora.mongodb.MongoContainer;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreMetadataFactory;
import org.apache.gora.store.impl.DataStoreMetadataAnalyzer;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Test case for MongoStoreDataAnalyzer
 */
public class TestMongoStoreMetadataAnalyzer<port> {
    private MongoContainer _container;
    private DataStoreMetadataAnalyzer storeMetadataAnalyzer;

    @Before
    public void setUp() throws GoraException, ClassNotFoundException {
        // Container for MongoStore
        this._container = new MongoContainer("4.2");
        _container.start();

        // Initiate the MongoDB server on the default port
        ServerAddress address = _container.getServerAddress();
        int port = address.getPort();
        String host = address.getHost();

        Properties prop = DataStoreFactory.createProps(); // для чего нужны prop

        // Store Mongo server "host:port" in Hadoop configuration
        // so that MongoStore will be able to get it latter
        Configuration conf = new Configuration();
        conf.set(MongoStoreParameters.PROP_MONGO_SERVERS, host + ":" + port);

        storeMetadataAnalyzer = DataStoreMetadataFactory.createAnalyzer(conf);
    }

    @Test
    public void TestGetType() {
        String actualType = storeMetadataAnalyzer.getType();
        String expectedType = "MONGODB";
        Assert.assertEquals(expectedType, actualType);
    }

    @Test
    public void TestGetTablesNames() {

    }

    @Test
    public void TestGetTableInfo(String tableName) {

    }

    /*
    Assert.assertTrue("Ignite Store Metadata Table Names", createAnalyzer.getTablesNames().equals(Lists.newArrayList("WEBPAGE", "EMPLOYEE")));
    IgniteTableMetadata tableInfo = (IgniteTableMetadata) createAnalyzer.getTableInfo("EMPLOYEE");
    Assert.assertEquals("Ignite Store Metadata Table Primary Key", "PKSSN", tableInfo.getPrimaryKey());
    HashMap<String, String> hmap = new HashMap();
    hmap.put("WEBPAGE", "VARBINARY");
    hmap.put("BOSS", "VARBINARY");
    hmap.put("SALARY", "INTEGER");
    hmap.put("DATEOFBIRTH", "BIGINT");
    hmap.put("PKSSN", "VARCHAR");
    hmap.put("VALUE", "VARCHAR");
    hmap.put("NAME", "VARCHAR");
    hmap.put("SSN", "VARCHAR");
    Assert.assertTrue("Ignite Store Metadata Table Columns", tableInfo.getColumns().equals(hmap));*/

    @After
    public void tearDown() {
        _container.stop();}
}
