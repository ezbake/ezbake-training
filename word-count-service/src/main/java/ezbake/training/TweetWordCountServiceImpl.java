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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.common.TokenUtils;

/**
 * A thrift service that counts the number of times a word is sent to this service via {@link #add(String,
 * EzSecurityToken)}.
 *
 * The words collected are not case sensitive. To determine the current count for a word, a caller
 * must call the {@link #getCount(String, EzSecurityToken)} method.
 *
 * The counts are not persistent and only represent an in-memory storage mechanism. Thus, when the service is stopped
 * all word counts are erased.
 */
public class TweetWordCountServiceImpl extends EzBakeBaseThriftService implements TweetWordCountService.Iface {
    private static final Logger logger = LoggerFactory.getLogger(TweetWordCountServiceImpl.class);

    /*
     * This map contains the count of words as sent to this service from
     * tweet-word-divide pipeline where the key is a unique, case-insensitive
     * word and the value is the number of times this service has been asked
     * to add the word.
     */
    private Map<String, Integer> wordCounts;

    public TweetWordCountServiceImpl() {
        wordCounts = new HashMap<>();
        logger.info("The tweet word count service was instantiated.");
    }

    public TProcessor getThriftProcessor() {
        return new TweetWordCountService.Processor<>(this);
    }

    public boolean ping() {
        return true;
    }

    public void add(String word, EzSecurityToken securityToken) throws TException {
        TokenUtils.validateSecurityToken(securityToken, this.getConfigurationProperties());

        if (StringUtils.isBlank(word)) {
            return;
        }

        String lcWord = word.trim().toLowerCase();
        if (wordCounts.containsKey(lcWord)) {
            wordCounts.put(lcWord, wordCounts.get(lcWord) + 1);
        } else {
            wordCounts.put(lcWord, 1);
        }
    }

    public int getCount(String word, EzSecurityToken securityToken) throws TException {
        TokenUtils.validateSecurityToken(securityToken, this.getConfigurationProperties());

        int wordCount = 0;
        if (StringUtils.isNotBlank(word) && wordCounts.containsKey(word.trim().toLowerCase())) {
            wordCount = wordCounts.get(word.trim().toLowerCase());
        }
        return wordCount;
    }
}
