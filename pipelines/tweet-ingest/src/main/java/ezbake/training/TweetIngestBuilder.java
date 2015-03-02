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

import ezbake.frack.api.Pipeline;
import ezbake.frack.api.PipelineBuilder;
import ezbake.frack.common.utils.INSUtil;
import ezbake.frack.common.utils.INSUtil.INSInfo;
import ezbake.frack.common.workers.BroadcastWorker;
import ezbake.frack.common.workers.SSRBroadcastWorker;
import ezbake.frack.common.workers.WarehausWorker;

public class TweetIngestBuilder implements PipelineBuilder {
    public static final String FEED_NAME = "tweet-ingest";

    public Pipeline build() {
        Pipeline pipeline = new Pipeline();
        TweetIngestGenerator generator = new TweetIngestGenerator();
        TweetIngestParser parser = new TweetIngestParser();

        INSInfo insInfo = INSUtil.getINSInfo(pipeline, FEED_NAME);
        RepositoryConverter repoConverter = new RepositoryConverter();
        repoConverter.setUriPrefix(insInfo.getUriPrefix());
        WarehausWorker<TweetWithRaw> warehausWorker = new WarehausWorker<>(
                TweetWithRaw.class, repoConverter, new VisibilityConverter());
        SSRConverter ssrConverter = new SSRConverter();
        ssrConverter.setUriPrefix(insInfo.getUriPrefix());
        SSRBroadcastWorker<TweetWithRaw> ssrWorker =
                new SSRBroadcastWorker<>(TweetWithRaw.class, ssrConverter);
        BroadcastWorker<TweetWithRaw> broadcastWorker = new BroadcastWorker<>(
                TweetWithRaw.class, insInfo.getTopics(), new TweetBroadcastConverter());

        pipeline.addGenerator(FEED_NAME + "_generator", generator);
        pipeline.addWorker(FEED_NAME + "_parser", parser);
        pipeline.addWorker(FEED_NAME + "_warehaus_worker", warehausWorker);
        pipeline.addWorker(FEED_NAME + "_broadcast_worker", broadcastWorker);
        pipeline.addWorker(FEED_NAME + "_ssr_worker", ssrWorker);

        pipeline.addConnection(FEED_NAME + "_generator", FEED_NAME + "_parser");
        pipeline.addConnection(FEED_NAME + "_parser", FEED_NAME + "_warehaus_worker");
        pipeline.addConnection(FEED_NAME + "_warehaus_worker", FEED_NAME + "_broadcast_worker");
        pipeline.addConnection(FEED_NAME + "_warehaus_worker", FEED_NAME + "_ssr_worker");

        return pipeline;
    }
}
