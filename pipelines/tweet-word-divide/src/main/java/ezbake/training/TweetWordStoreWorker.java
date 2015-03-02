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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.ThriftClient;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;

/**
 * The pipeline worker that receives a word from a tweet and stores the word to the Tweet Word Count Thrift service.
 */
public class TweetWordStoreWorker extends Worker<TweetWord> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TweetWordStoreWorker.class);

    private ThriftClientPool pool;
    private TweetWordCountService.Client serviceClient;
    private EzbakeSecurityClient securityClient;
    private Properties properties;

    public TweetWordStoreWorker() {
        super(TweetWord.class);
    }

    /**
     * Initializes the worker for processing.
     *
     * @param properties The EzBake configuration values for the running environment.
     */
    public void initialize(Properties properties) {
        super.initialize(properties);

        this.properties = properties;
        securityClient = new EzbakeSecurityClient(properties);
        pool = new ThriftClientPool(properties);
        logger.info("Initialization is completed. Properties: {}", properties);
    }

    /**
     * Cleans up the worker by returning and closing open service resources.
     */
    public void cleanup() {
        super.cleanup();
        ThriftClient.close();
    }

    /**
     * Performs processing on the TweetWord object by adding it to the Word Count service.
     *
     * @param visibility The Visibility containing the Accumulo visibility string representing the classification level
     * of the data contained in the incoming thrift data object.
     * @param object The incoming Thrift object to be processed.
     */
    @Override
    public void process(Visibility visibility, TweetWord object) {
        if (object != null && object.getWord() != null) {
            try {
                final EzSecurityToken token;
                try {
                    token = securityClient.fetchAppToken();
                } catch (TException e) {
                    logger.error(
                            "An error occurred while obtaining the security token: {}\nProperties Dump: {}",
                            e.getMessage(), this.properties);
                    logger.error("", e);
                    throw new RuntimeException(e);
                }

                serviceClient = pool.getClient(
                        EzBakeTrainingConstants.WORD_COUNT_SERVICE_NAME, TweetWordCountService.Client.class);

                serviceClient.add(object.getWord(), token);
                logger.info(
                        "Added word '{}' to tweet word count service.", object.getWord());
            } catch (TException e) {
                logger.error(
                        "An error occurred when adding the word '{}' to the TweetWordCountService", object.getWord());
                logger.error("", e);
            } finally {
                pool.returnToPool(serviceClient);
            }
        }
    }
}
