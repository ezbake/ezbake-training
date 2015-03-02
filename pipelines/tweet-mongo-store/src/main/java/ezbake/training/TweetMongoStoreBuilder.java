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

import ezbake.frack.api.Listener;
import ezbake.frack.api.Pipeline;
import ezbake.frack.api.PipelineBuilder;

public class TweetMongoStoreBuilder implements PipelineBuilder {
    private static final String SOURCE_TOPIC = "tweet-ingestTopic1";
    private static final String PIPELINE_WORKER = "tweet-mongo-store-worker";
    private static final String PIPELINE_LISTENER = "tweet-mongo-store-listener";

    public Pipeline build() {
        Pipeline pipeline = new Pipeline();

        Listener<Tweet> listener = new Listener<>(Tweet.class);
        listener.registerListenerTopic(SOURCE_TOPIC);

        TweetMongoStoreWorker worker = new TweetMongoStoreWorker();
        pipeline.addWorker(PIPELINE_WORKER, worker);
        pipeline.addListener(PIPELINE_LISTENER, listener);
        pipeline.addConnection(PIPELINE_LISTENER, PIPELINE_WORKER);

        return pipeline;
    }
}
