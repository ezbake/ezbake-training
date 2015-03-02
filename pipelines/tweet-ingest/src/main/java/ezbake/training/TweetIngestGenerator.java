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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Generator;
import ezbake.frack.common.utils.StackTraceUtil;
import ezbake.frack.core.data.thrift.StreamEvent;
import ezbake.quarantine.thrift.AdditionalMetadata;
import ezbake.quarantine.thrift.MetadataEntry;

/**
 * The pipeline reads tweets from a file located in the tweets.folder and emits them for downstream consumers.
 * <p/>
 * Properties:
 * <ul>
 *     <li> tweets.folder - The directory where the tweet files are stored.</li>
 *     <li> processing.runOnce - Indicates how this ingest processes. A value of true will process all files
 *          sitting in the tweets.folder directory one time and then cease processing. In addition, the
 *          processed files will not be deleted. If a value of false or no value is given then each file is
 *          deleted after processing it. Further, files may be added to the tweets.folder and picked up by the
 *          processing automatically.</li>
 *     <li> processing.pauseMilliseconds - Indicates the amount of time, in milliseconds, between runs. No pause
 *          occurs if this value is not provided.</li>
 * </ul>
 */
public class TweetIngestGenerator extends Generator<StreamEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TweetIngestGenerator.class);

    private Random randomGenerator = new Random();
    private File folder;
    private long pauseMilliseconds = 0L;
    private boolean runOnce = false;
    private boolean isFirstRun = true;

    public void initialize(Properties properties) {
        EzProperties props = new EzProperties(properties, true);
        logger.info("Reading tweets data from " + props.get("tweets.folder"));

        folder = new File(String.valueOf(props.get("tweets.folder")));
        pauseMilliseconds = props.getLong("processing.pauseMilliseconds", this.pauseMilliseconds);
        runOnce = props.getBoolean("processing.runOnce", this.runOnce);
    }

    public void generate() {
        if (shouldProcessingContinue()) {
            processFiles();
            performPostProcessTasks();
        }
        pause();
    }

    private void processFiles() {
        File[] files = folder.listFiles();
        if (files != null && files.length > 0) {
            File fileToProcess = files[0];
            try {
                logger.info("Processing file: {}", fileToProcess.getAbsolutePath());
                BufferedReader br = new BufferedReader(new FileReader(fileToProcess));
                String line;
                int recordCount = 0;
                while ((line = br.readLine()) != null) {
                    logger.info("Processing item #{}: {}", recordCount + 1, line);
                    emitEvent(line);
                    recordCount++;
                }
                br.close();
                logger.info("Done Processing file: {} - {} records read", fileToProcess.getAbsolutePath(), recordCount);
                if (!runOnce) {
                    boolean deleteOkay = fileToProcess.delete();
                    if (!deleteOkay) {
                        logger.error("Could not successfully delete {}", fileToProcess.getAbsolutePath());
                    }
                }
            } catch (IOException e) {
                logger.error("Error reading file:: {}", fileToProcess.getAbsolutePath(), e);
                AdditionalMetadata metaData = new AdditionalMetadata();
                MetadataEntry stackTraceEntry = new MetadataEntry();
                stackTraceEntry.setValue(StackTraceUtil.getStackTrace(e));
                metaData.putToEntries("stackTrace", stackTraceEntry);
                try {
                    sendRawToQuarantine(
                            Files.toByteArray(fileToProcess), new Visibility().setFormalVisibility("U"),
                            "Could not read twitter file", metaData);
                } catch (IOException ioe) {
                    logger.error("FATAL, cannot send object to Quarantine.", ioe);
                    throw new RuntimeException("Could not send object to Quarantine.", ioe);
                }
            }
        }
    }

    private void emitEvent(String status) {
        StreamEvent event = new StreamEvent();
        event.setOrigin("twitter-firehose");
        event.setDateTime(new Date().toString());

        int random = randomGenerator.nextInt(100);
        if (random % 5 == 0) {
            event.setAuthorization("TS");
        } else if (random % 3 == 0) {
            event.setAuthorization("S");
        } else if (random % 2 == 0) {
            event.setAuthorization("C");
        } else {
            event.setAuthorization("U");
        }

        try {
            event.setContent(status.getBytes("UTF-8"));
            logger.info("Emitting stream event: {}" + event);
            Visibility vis = new Visibility().setFormalVisibility(event.getAuthorization());
            outputToPipes(vis, event);
        } catch (IOException e) {
            logger.error("Error writing tweet - " + status);
        }
    }

    private boolean shouldProcessingContinue() {
        return !runOnce || runOnce && isFirstRun;
    }

    private void performPostProcessTasks() {
        if (isFirstRun) {
            isFirstRun = false;
        }
    }

    private void pause() {
        if (pauseMilliseconds > 0) {
            try {
                Thread.sleep(pauseMilliseconds);
            } catch (InterruptedException e) {
                // Ignored
            }
        }
    }
}
