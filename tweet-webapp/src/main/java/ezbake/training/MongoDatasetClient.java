/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.training;

import java.util.List;

import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.data.common.ThriftClient;
import ezbake.data.common.classification.VisibilityUtils;
import ezbake.data.mongo.thrift.EzMongo;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;

public class MongoDatasetClient {
    private static final String EZMONGO_SERVICE_NAME = "ezmongo";
    private static final Logger logger = LoggerFactory.getLogger(MongoDatasetClient.class);

    private static MongoDatasetClient instance;

    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;

    private MongoDatasetClient() {
        createClient();
    }

    public static MongoDatasetClient getInstance() {
        if (instance == null) {
            instance = new MongoDatasetClient();
        }
        return instance;
    }

    public EzMongo.Client getThriftClient() throws TException {
        return pool.getClient(EZMONGO_SERVICE_NAME, EzMongo.Client.class);
    }

    public void close() throws Exception {
        ThriftClient.close();
    }

    public void createIndex(String collectionName, String jsonKeys, String jsonOptions) throws TException {
        EzMongo.Client c = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            c = getThriftClient();
            logger.info("Calling EzMongo creating index for {}...", collectionName);
            c.createIndex(collectionName, jsonKeys, jsonOptions, token);
            logger.info("Index created.");
        } finally {
            if (c != null) {
                pool.returnToPool(c);
            }
        }
    }

    public List<String> getIndexInfo(String collectionName) throws TException {
        EzMongo.Client c = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            c = getThriftClient();

            logger.info("Calling EzMongo getting index info for {}...", collectionName);
            return c.getIndexInfo(collectionName, token);
        } finally {
            if (c != null) {
                pool.returnToPool(c);
            }
        }
    }

    public boolean collectionExists(String collectionName) throws TException {
        EzMongo.Client c = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            c = getThriftClient();

            logger.info("Calling EzMongo checking collection {}...", collectionName);
            boolean exists = c.collectionExists(collectionName, token);
            logger.info("collection {} exists: {}", collectionName, exists);

            return exists;
        } finally {
            if (c != null) {
                pool.returnToPool(c);
            }
        }
    }

    public void createCollection(String collectionName) throws TException {
        EzMongo.Client c = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            c = getThriftClient();

            logger.info("Calling EzMongo creating collection {}...", collectionName);
            c.createCollection(collectionName, token);
            logger.info("Created collection {}", collectionName);
        } finally {
            if (c != null) {
                pool.returnToPool(c);
            }
        }
    }

    public List<String> searchText(String collectionName, String searchText) throws TException {
        EzMongo.Client c = null;
        List<String> results = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            c = getThriftClient();

            logger.info("Calling EzMongo searching text for {}...", searchText);
            results = c.textSearch(collectionName, searchText, token);
            logger.info("Text search results: {}", results);
        } finally {
            if (c != null) {
                pool.returnToPool(c);
            }
        }
        return results;
    }

    public int getWordCount(String searchText) throws TException {
        int wordCount = 0;
        TweetWordCountService.Client tweetWordCountClient = null;

        try {
            EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

            tweetWordCountClient = pool.getClient(
                    EzBakeTrainingConstants.WORD_COUNT_SERVICE_NAME, TweetWordCountService.Client.class);

            logger.info("Calling tweet word count service...");
            wordCount = tweetWordCountClient.getCount(searchText, token);
            logger.info("wordCount: {}", wordCount);
        } finally {
            if (tweetWordCountClient != null) {
                pool.returnToPool(tweetWordCountClient);
            }
        }
        return wordCount;
    }

    public void validateVisibility(String formalVisibility) throws VisibilityParseException {
        VisibilityUtils.generateVisibilityList(formalVisibility);
    }

    void createClient() {
        try {
            EzConfiguration configuration = new EzConfiguration();
            logger.info("in createClient, configuration: {}", configuration.getProperties());

            securityClient = new EzbakeSecurityClient(configuration.getProperties());
            pool = new ThriftClientPool(configuration.getProperties());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
