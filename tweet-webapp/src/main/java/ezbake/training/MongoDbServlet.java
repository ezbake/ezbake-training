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
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import ezbake.base.thrift.Coordinate;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.EzConfiguration;
import ezbake.data.common.TimeUtil;
import ezbake.data.mongo.redact.RedactHelper;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.ins.thrift.gen.InternalNameService;
import ezbake.ins.thrift.gen.InternalNameServiceConstants;
import ezbake.publisher.thrift.ContentPublisher;
import ezbake.publisher.thrift.ContentPublisherServiceConstants;
import ezbake.publisher.thrift.PublishData;
import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;
import ezbake.warehaus.UpdateEntry;

public class MongoDbServlet extends HttpServlet {
    public static final String COLLECTION_NAME = "tweets";
    public static final String USER_FIELD_NAME = "user";
    public static final String USERNAME_FIELD_NAME = "username";
    public static final String SCREEN_NAME_FIELD_NAME = "screenName";
    public static final String SECONDARY_SCREEN_NAME_FIELD_NAME = "screen_name";
    public static final String TWEET_TEXT_FIELD_NAME = "text";

    private static final long serialVersionUID = 9051600090960237717L;
    private static final String WORD_COUNT_COLOR_PROPERTY = "example.web.twitter.wordcount.color";

    protected static Logger logger = LoggerFactory.getLogger(MongoDbServlet.class);

    private static EzbakeSecurityClient securityClient;
    private ThriftClientPool pool;
    private long idCount = 0;

    public void destroy() {
        try {
            MongoDatasetClient.getInstance().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void init() throws ServletException {
        try {
            MongoDatasetClient client = MongoDatasetClient.getInstance();

            logger.info("Initializing mongo db servlet, COLLECTION_NAME: {}", COLLECTION_NAME);

            // if collection doesn't exist, create it.
            if (!client.collectionExists(COLLECTION_NAME)) {
                logger.info("collection doesn't exist, we need to create the collection.");
                client.createCollection(COLLECTION_NAME);
            }

            createTextIndex(client);

            final Properties props = new EzConfiguration().getProperties();
            pool = new ThriftClientPool(props);
            securityClient = new EzbakeSecurityClient(props);
        } catch (Exception e) {
            logger.error("Error during initialization", e);
            throw new ServletException(e.getMessage());
        }
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doPost(request, response);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String action = request.getParameter("action");

        String result;
        if ("insertTweet".equalsIgnoreCase(action)) {
            result = insertTweet(request, response);
        } else if ("searchTweet".equalsIgnoreCase(action)) {
            result = searchTweet(request, response);
        } else if ("getWordCount".equalsIgnoreCase(action)) {
            result = getWordCount(request, response);
        } else if ("validateVisibility".equalsIgnoreCase(action)) {
            result = validateVisibility(request, response);
        } else {
            result = "Unknown action: " + action;
        }

        response.setHeader("Content-Type", "text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println(result);
    }

    /**
     * Creates a text index on the "text" field if it doesn't exist
     *
     * @throws TException
     */
    private void createTextIndex(MongoDatasetClient client) throws TException {
        boolean hasTextIndex = false;
        String namespace = null;

        logger.info("getting index info..");

        List<String> indexList = client.getIndexInfo(COLLECTION_NAME);
        for (String index : indexList) {
            logger.info("we have an index: {}", index);

            DBObject indexObj = (DBObject) JSON.parse(index);
            String indexName = (String) indexObj.get("name");
            if (namespace == null) {
                namespace = (String) indexObj.get("ns");
            }

            if (indexName.equals(TWEET_TEXT_FIELD_NAME + "_text")) {
                hasTextIndex = true;
            }
        }

        if (!hasTextIndex) {
            DBObject obj = new BasicDBObject();
            // we are putting a text index on the "text" field in the mongo collection
            obj.put(TWEET_TEXT_FIELD_NAME, "text");
            String jsonKeys = JSON.serialize(obj);

            logger.info(
                    "creating text index with jsonKeys: {}, COLLECTION_NAME: {}", jsonKeys, COLLECTION_NAME);

            client.createIndex(COLLECTION_NAME, jsonKeys, null);

            logger.info("MongoDbServlet: created text index: {}", jsonKeys);
        } else {
            logger.info("MongoDbServlet: we already have the text index.");
        }
    }

    private String getWordCount(HttpServletRequest request, HttpServletResponse response) {
        String searchText = request.getParameter("searchText");
        String result;

        try {
            MongoDatasetClient client = MongoDatasetClient.getInstance();
            int wordCount = client.getWordCount(searchText);

            // get wordCount's text color from EzConfiguration for demonstration.
            // if the word count property is not found in EzConfiguration then it
            // defaults to red.
            EzConfiguration ezConfiguration = new EzConfiguration();

            String wordCountColor = ezConfiguration.getProperties().getProperty(WORD_COUNT_COLOR_PROPERTY, "red");
            logger.info("wordCountColor: {}", wordCountColor);

            result = String.format("<span style='color:%s'>%d</span>", wordCountColor, wordCount);
        } catch (Exception e) {
            result = "Unable to get word count: " + e.getMessage();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        return result;
    }

    private String searchTweet(HttpServletRequest request, HttpServletResponse response) {
        String searchText = request.getParameter("searchText");
        String result;

        try {
            MongoDatasetClient client = MongoDatasetClient.getInstance();
            createTextIndex(client);

            logger.info("searchText: {}", searchText);

            List<String> data = client.searchText(COLLECTION_NAME, searchText);

            if (data.size() == 0) {
                result = "No results found";
            } else {
                StringBuilder buffer = new StringBuilder();
                for (String tweetJSON : data) {
                    buffer.append("<tr>");

                    DBObject dbObj = (DBObject) JSON.parse(tweetJSON);
                    String _id = dbObj.get("_id").toString();
                    String id = null;
                    // The "id" field can be a Long or Integer;
                    //   it will be Integer if content-publisher was invoked from the tweet webapp
                    //   to insert the tweet into mongo.
                    Object idObj = dbObj.get("id");
                    if (idObj != null && (idObj instanceof Long || idObj instanceof Integer)) {
                        id = idObj.toString();
                    }

                    DBObject formalVisibilityObj = (DBObject) dbObj.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
                    String formalVisibility = null;
                    if (formalVisibilityObj != null) {
                        formalVisibility = formalVisibilityObj.toString();
                    }

                    DBObject userObj = (DBObject) dbObj.get(USER_FIELD_NAME);
                    String userName;
                    if (userObj != null) {
                        userName = (String) userObj.get(SCREEN_NAME_FIELD_NAME);
                        if (userName == null) {
                            // it's possible that the tweets were ingested with a
                            // different field name for the screen name.
                            userName = (String) userObj.get(SECONDARY_SCREEN_NAME_FIELD_NAME);
                        }
                    } else { // try a different field name for the username
                        userName = (String) dbObj.get(USERNAME_FIELD_NAME);
                    }

                    String tweet = (String) dbObj.get(TWEET_TEXT_FIELD_NAME);

                    // construct the columns to display on the jsp
                    buffer.append("<td>");
                    buffer.append(_id);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append(id);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append(formalVisibility);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append("@");
                    buffer.append(userName);
                    buffer.append(": ");
                    buffer.append(tweet);
                    buffer.append("</td>");

                    buffer.append("</tr>");
                }

                result = buffer.toString();
                logger.info(result);
            }
        } catch (Exception e) {
            result = "Unable to retrieve any results: " + e.getMessage();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        return result;
    }

    private String insertTweet(HttpServletRequest request, HttpServletResponse response) {
        String userName = request.getParameter("userName");
        String CAPCO = request.getParameter("CAPCO");
        String tweetContent = request.getParameter("tweetContent");
        String result = null;
        ContentPublisher.Client client = null;
        String feedName = "tweet-ingest";
        try {
            logger.info("Initiating request to Content Publisher Service");
            client = pool.getClient(ContentPublisherServiceConstants.SERVICE_NAME, ContentPublisher.Client.class);
            PublishData data = new PublishData();
            Tweet tweet = new Tweet();
            tweet.setTimestamp(System.currentTimeMillis());
            tweet.setId(idCount++);
            tweet.setText(tweetContent);
            tweet.setUserId(1);
            tweet.setUserName(userName);
            tweet.setIsFavorite(new Random().nextBoolean());
            tweet.setIsRetweet(new Random().nextBoolean());
            UpdateEntry entry = new UpdateEntry(getUriPrefix(feedName, getToken()) + tweet.getId());
            entry.setRawData(new TSerializer(new TSimpleJSONProtocol.Factory()).serialize(tweet));
            entry.setParsedData(ThriftUtils.serialize(tweet));

            data.setEntry(entry);
            data.setFeedname(feedName);

            SSRJSON ssrJson = new SSRJSON();
            ssrJson.setJsonString(new TSerializer(new TSimpleJSONProtocol.Factory()).toString(tweet));
            SSR ssr = new SSR();
            ssr.setUri(entry.getUri());
            ssr.setTitle(String.valueOf(tweet.getId()));
            ssr.setVisibility(new Visibility().setFormalVisibility("U"));
            ssr.setSnippet(tweet.getText());
            if (tweet.getGeoLocation() != null) {
                Coordinate coordinate = new Coordinate();
                coordinate.setLatitude(tweet.getGeoLocation().getLatitude());
                coordinate.setLongitude(tweet.getGeoLocation().getLongitude());
            }
            ssr.setResultDate(TimeUtil.convertToThriftDateTime(tweet.getTimestamp()));
            ssrJson.setSsr(ssr);
            data.setSsrjson(ssrJson);
            client.publish(data, new Visibility().setFormalVisibility(CAPCO), getToken());
            logger.info("Sent Tweet to the Content Publisher Service");
            result = "Successfully added the tweet(id=" + tweet.getId() + ")";
        } catch (IOException | TException e) {
            result = "Failed to insert data: " + e.getMessage();
            logger.error("Failed to insert data", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            pool.returnToPool(client);
        }

        return result;
    }

    private String validateVisibility(HttpServletRequest request, HttpServletResponse response) {
        String formalVisibility = request.getParameter("formalVisibility");
        String result;

        try {
            logger.info("validateVisibility: formalVisibility: {}", formalVisibility);
            MongoDatasetClient.getInstance().validateVisibility(formalVisibility);
            result = String.format("Successfully validated formal visibility '%s'", formalVisibility);
            logger.info(result);
        } catch (VisibilityParseException e) {
            result =
                    String.format("Error when validating formal visibility '%s': %s", formalVisibility, e.getMessage());

            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }

        return result;
    }

    private EzSecurityToken getToken() throws EzSecurityTokenException, IOException {
        return securityClient.fetchTokenForProxiedUser();
    }

    private String getUriPrefix(String feedName, EzSecurityToken token) throws TException {
        InternalNameService.Client insClient = null;
        String applicationSecurityId = new EzSecurityTokenWrapper(token).getApplicationSecurityId();

        try {
            insClient = pool.getClient(InternalNameServiceConstants.SERVICE_NAME, InternalNameService.Client.class);
            return insClient.getURIPrefix(applicationSecurityId, feedName);
        } catch (Exception ex) {
            logger.error("Failed to communicate with INS", ex);
            throw new TException("Failed to communicate with INS", ex);
        } finally {
            pool.returnToPool(insClient);
        }
    }
}
