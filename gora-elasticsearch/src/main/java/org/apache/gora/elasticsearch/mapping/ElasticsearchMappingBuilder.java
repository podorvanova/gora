/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.elasticsearch.mapping;

import com.google.inject.ConfigurationException;
import org.apache.gora.elasticsearch.store.ElasticsearchStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.util.GoraException;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Builder for Mapping definitions of Elasticsearch.
 */
public class ElasticsearchMappingBuilder<K, T extends PersistentBase> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchMappingBuilder.class);

    // Index description
    static final String ATT_NAME = "name";

    static final String ATT_TYPE = "type";

    // Class description
    static final String TAG_CLASS = "class";

    static final String ATT_KEYCLASS = "keyClass";

    static final String ATT_INDEX = "index";

    static final String TAG_FIELD = "field";

    static final String ATT_DOCFIELD = "docfield";

    /**
     * Mapping instance being built.
     */
    private ElasticsearchMapping elasticsearchMapping;

    private final ElasticsearchStore<K, T> dataStore;

    /**
     * Constructor for ElasticsearchMappingBuilder.
     *
     * @param store ElasticsearchStore instance
     */
    public ElasticsearchMappingBuilder(final ElasticsearchStore<K, T> store) {
        this.elasticsearchMapping = new ElasticsearchMapping();
        this.dataStore = store;
    }

    /**
     * Returns the Elasticsearch Mapping being built.
     *
     * @return Elasticsearch Mapping instance
     */
    public ElasticsearchMapping getElasticsearchMapping() {
        return elasticsearchMapping;
    }

    /**
     * Sets the Elasticsearch Mapping.
     *
     * @param elasticsearchMapping Elasticsearch Mapping instance
     */
    public void setElasticsearchMapping(ElasticsearchMapping elasticsearchMapping) {
        this.elasticsearchMapping = elasticsearchMapping;
    }

    /**
     * Reads Elasticsearch mappings from file.
     *
     * @param inputStream Mapping input stream
     */
    public void readMappingFile(InputStream inputStream) {
        try {
            SAXBuilder saxBuilder = new SAXBuilder();
            if (inputStream == null) {
                LOG.error("The mapping input stream is null!");
                throw new GoraException("The mapping input stream is null!");
            }
            Document document = saxBuilder.build(inputStream);
            if (document == null) {
                LOG.error("The mapping document is null!");
                throw new GoraException("The mapping document is null!");
            }

            Element root = document.getRootElement();
            // Extract class descriptions
            @SuppressWarnings("unchecked")
            List<Element> classElements = root.getChildren(TAG_CLASS);
            for (Element classElement : classElements) {
                final Class<T> persistentClass = dataStore.getPersistentClass();
                final Class<K> keyClass = dataStore.getKeyClass();
                if (haveKeyClass(keyClass, classElement)
                        && havePersistentClass(persistentClass, classElement)) {
                    loadPersistentClass(classElement, persistentClass);
                    break;
                }
            }


        } catch (IOException | JDOMException | ConfigurationException ex) {
            throw new RuntimeException(ex);
        }
        LOG.info("Gora Elasticsearch mapping file was read successfully.");
    }

    private boolean haveKeyClass(final Class<K> keyClass,
                                 final Element classElement) {
        return classElement.getAttributeValue(ATT_KEYCLASS).equals(
                keyClass.getName());
    }

    private boolean havePersistentClass(final Class<T> persistentClass,
                                        final Element classElement) {
        return classElement.getAttributeValue(ATT_NAME).equals(
                persistentClass.getName());
    }

    /**
     * Handle the XML parsing of the class definition.
     *
     * @param classElement the XML node containing the class definition
     */
    protected void loadPersistentClass(Element classElement,
                                       Class<T> pPersistentClass) {

        String indexNameFromMapping = classElement.getAttributeValue(ATT_INDEX);
        String indexName = dataStore.getSchemaName(indexNameFromMapping, pPersistentClass);

        elasticsearchMapping.setIndexName(indexName);
        // docNameFromMapping could be null here
        if (!indexName.equals(indexNameFromMapping)) {
            ElasticsearchStore.LOG
                    .info("Keyclass and nameclass match but mismatching index names "
                            + " mappingfile schema is '" + indexNameFromMapping
                            + "' vs actual schema '" + indexName
                            + "' , assuming they are the same.");
            if (indexNameFromMapping != null) {
                elasticsearchMapping.setIndexName(indexName);
            }
        }

        // Process fields declaration
        @SuppressWarnings("unchecked")
        List<Element> fields = classElement.getChildren(TAG_FIELD);
        for (Element fieldElement : fields) {
            String fieldTypeName = fieldElement.getAttributeValue(ATT_TYPE).toUpperCase(Locale.getDefault());
            Field.FieldType fieldType = new Field.FieldType(Field.DataType.valueOf(fieldTypeName));
            Field field = new Field(fieldElement.getAttributeValue(ATT_DOCFIELD), fieldType);
            elasticsearchMapping.addField(fieldElement.getAttributeValue(ATT_NAME), field);
        }
    }
}