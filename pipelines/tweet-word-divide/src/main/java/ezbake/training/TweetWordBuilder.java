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

/**
 * Constructs the tweet-word-divide pipeline.
 *
 * The pipeline listens for a tweet from the tweet-ingest pipeline, divides the tweet text into words and saves the
 * word to the word count service.
 */
public class TweetWordBuilder implements PipelineBuilder {
    /**
     * The topic to which this pipeline is listening.
     */
    private static final String SOURCE_TOPIC = "tweet-ingestTopic1";

    /**
     * The unique name identifying the Listener that listens for the tweets emitted from the tweet-ingest pipeline.
     */
    private static final String PIPELINE_LISTENER = "tweet-word-listener";

    /**
     * The unique name identifying the Worker that will divide the tweet text into words.
     */
    private static final String PIPELINE_DIVIDE_WORKER = "tweet-word-divide-worker";

    /**
     * The unique name identifying the Worker that will receive the words from the divide worker and store the word to
     * the word count thrift service.
     */
    private static final String PIPELINE_STORE_WORKER = "tweet-word-store-worker";

    /**
     * Creates the pipeline by identifying the pipelines components (i.e., listeners, workers, and others as needed) and
     * connects the various components together to form a pipeline.
     */
    public Pipeline build() {
        Pipeline pipeline = new Pipeline();

        Listener<Tweet> listener = new Listener<>(Tweet.class);
        listener.registerListenerTopic(SOURCE_TOPIC);

        pipeline.addWorker(PIPELINE_DIVIDE_WORKER, new TweetWordDivideWorker());
        pipeline.addWorker(PIPELINE_STORE_WORKER, new TweetWordStoreWorker());

        pipeline.addListener(PIPELINE_LISTENER, listener);
        pipeline.addConnection(PIPELINE_LISTENER, PIPELINE_DIVIDE_WORKER);
        pipeline.addConnection(PIPELINE_DIVIDE_WORKER, PIPELINE_STORE_WORKER);

        return pipeline;
    }
}
