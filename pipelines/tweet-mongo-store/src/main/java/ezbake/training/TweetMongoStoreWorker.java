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

import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.ThriftClient;
import ezbake.data.mongo.thrift.EzMongo;
import ezbake.data.mongo.thrift.MongoEzbakeDocument;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;

public class TweetMongoStoreWorker extends Worker<Tweet> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TweetMongoStoreWorker.class);
    private static final String EZMONGO = "ezmongo";

    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;
    private Properties properties;
    private int count = 0;

    public TweetMongoStoreWorker() {
        super(Tweet.class);
    }

    public void initialize(Properties properties) {
        super.initialize(properties);

        this.properties = properties;
        securityClient = new EzbakeSecurityClient(properties);
        pool = new ThriftClientPool(properties);
        logger.info("Initialization is completed. Properties: {}", properties);
    }

    public void cleanup() {
        super.cleanup();
        ThriftClient.close();
    }

    @Override
    public void process(Visibility visibility, Tweet tweetThrift) {
        logger.info("Processing thrift object #{}", ++count);
        try {
            insertTweet(visibility, tweetThrift);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void insertTweet(Visibility visibility, Tweet tweet) throws TException {
        EzMongo.Client mongoClient = null;
        try {
            mongoClient = pool.getClient(EZMONGO, EzMongo.Client.class);

            TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
            String jsonContent = serializer.toString(tweet);

            final EzSecurityToken token;
            try {
                token = securityClient.fetchAppToken();
            } catch (TException e) {
                logger.error(
                        "An error occurred while obtaining the security token: {}\nProperties Dump: {}", e.getMessage(),
                        this.properties);
                logger.error("", e);
                throw new RuntimeException(e);
            }

            String result = mongoClient.insert("tweets", new MongoEzbakeDocument(jsonContent, visibility), token);
            logger.info("Successful mongo client insert {} with visibility {}", result, visibility);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new TException(e);
        } finally {
            pool.returnToPool(mongoClient);
        }
    }
}
