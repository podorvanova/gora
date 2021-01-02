/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.elasticsearch.store;

import org.apache.gora.elasticsearch.mapping.ElasticsearchMapping;
import org.apache.gora.elasticsearch.mapping.Field;
import org.apache.gora.elasticsearch.utils.ElasticsearchParameters;
import org.apache.gora.examples.generated.Employee;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test case for ElasticsearchStore.
 */
public class TestElasticsearchStore {

    @Test
    public void testInitialize() throws GoraException {
        Configuration conf = new Configuration();
        ElasticsearchStore<String, Employee> store =
                DataStoreFactory.createDataStore(ElasticsearchStore.class, String.class, Employee.class, conf);
        ElasticsearchMapping mapping = store.getMapping();

        Map<String, Field> fields = new HashMap<String, Field>() {{
            put("name", new Field("name", new Field.FieldType(Field.DataType.TEXT)));
            put("dateOfBirth", new Field("dateOfBirth", new Field.FieldType(Field.DataType.LONG)));
            put("ssn", new Field("ssn", new Field.FieldType(Field.DataType.TEXT)));
            put("value", new Field("value", new Field.FieldType(Field.DataType.TEXT)));
            put("salary", new Field("salary", new Field.FieldType(Field.DataType.INTEGER)));
            put("boss", new Field("boss", new Field.FieldType(Field.DataType.OBJECT)));
            put("webpage", new Field("webpage", new Field.FieldType(Field.DataType.OBJECT)));
        }};

        Assert.assertEquals("frontier", store.getSchemaName());
        Assert.assertEquals("frontier", mapping.getIndexName());
        Assert.assertEquals(fields, mapping.getFields());
    }

    @Test
    public void testLoadElasticsearchParameters() throws IOException {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("gora.properties"));

        ElasticsearchParameters parameters = ElasticsearchParameters.load(properties, conf);

        Assert.assertEquals("localhost", parameters.getHost());
        Assert.assertEquals(9200, parameters.getPort());
        Assert.assertEquals("BASIC", parameters.getAuthenticationMethod());
        Assert.assertEquals("username", parameters.getUsername());
        Assert.assertEquals("password", parameters.getPassword());
    }
}