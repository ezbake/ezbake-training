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

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.Coordinate;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class TweetIngestParser extends Worker<StreamEvent> {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(TweetIngestParser.class);

    public TweetIngestParser() {
        super(StreamEvent.class);
    }

    @Override
    public void process(Visibility visibility, StreamEvent streamEvent) {
        try {
            Status status = DataObjectFactory.createStatus(new String(streamEvent.getContent(), "UTF-8"));
            Tweet tweet = this.convertToTweet(status);
            String raw = streamEvent.getContent().toString();
            TweetWithRaw thriftAndRaw = new TweetWithRaw(tweet, raw);

            outputToPipes(visibility, thriftAndRaw);
        } catch (TwitterException | IOException e) {
            logger.error("Error during tweet output to pipes", streamEvent, e);

            AdditionalMetadata metaData = new AdditionalMetadata();
            MetadataEntry stackTraceEntry = new MetadataEntry();
            stackTraceEntry.setValue(StackTraceUtil.getStackTrace(e));
            metaData.putToEntries("stackTrace", stackTraceEntry);
            try {
                sendObjectToQuarantine(
                        streamEvent, visibility, "Error during tweet output to pipes", metaData);
            } catch (IOException ioe) {
                logger.error("FATAL, cannot send object to Quarantine.", ioe);
                throw new RuntimeException("Could not send object to Quarantine.", ioe);
            }
        }
    }

    public void initialize(Properties props) {
    }

    private Tweet convertToTweet(Status status) {
        Tweet tweet = new Tweet();
        tweet.setTimestamp(status.getCreatedAt().getTime());
        tweet.setId(status.getId());
        tweet.setText(status.getText());
        tweet.setUserId(status.getUser().getId());
        tweet.setUserName(status.getUser().getName());
        tweet.setIsFavorite(status.isFavorited());
        tweet.setIsRetweet(status.isRetweet());
        if (status.getGeoLocation() != null) {
            tweet.setGeoLocation(
                    new Coordinate(status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude()));
        }
        return tweet;
    }
}
