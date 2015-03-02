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
import java.text.BreakIterator;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;

/**
 * The pipeline worker that receives a {@link ezbake.training.Tweet} instance and divides the text into
 * words before outputting them as {@link ezbake.training.TweetWord} for downstream workers or listeners.
 */
public class TweetWordDivideWorker extends Worker<Tweet> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TweetWordDivideWorker.class);

    public TweetWordDivideWorker() {
        super(Tweet.class);
    }

    /**
     * Performs processing on the Tweet object by dividing the tweet's text into words.
     *
     * @param visibility The Visibility containing the Accumulo visibility string representing the classification level
     * of the data contained in the incoming thrift data object.
     * @param data The incoming Thrift object to be processed.
     */
    @Override
    public void process(Visibility visibility, Tweet data) {
        if (data != null && data.getText() != null) {
            BreakIterator wordIterator = BreakIterator.getWordInstance();
            wordIterator.setText(data.getText());

            int wordStart = wordIterator.first();
            int wordEnd = wordIterator.next();
            for (; wordEnd != BreakIterator.DONE; wordStart = wordEnd, wordEnd = wordIterator.next()) {
                String tweetTextWord = data.getText().substring(wordStart, wordEnd);
                if (StringUtils.isNotBlank(tweetTextWord)) {
                    try {
                        outputResultsToPipe(visibility, tweetTextWord);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Output and/or broadcast the tweet word for downstream pipes and listeners.
     *
     * @param word A word from the tweet.
     * @param visibility Visibility containing the Accumulo visibility string representing the classification level of
     * the data contained in the incoming thrift data object.
     * @throws IOException
     */
    private void outputResultsToPipe(Visibility visibility, String word) throws IOException {
        TweetWord tweetWord = new TweetWord();
        tweetWord.setWord(word);

        outputToPipes(visibility, tweetWord);
        logger.info("Output to pipes (with visibility {}): {}", visibility, tweetWord);
    }
}
