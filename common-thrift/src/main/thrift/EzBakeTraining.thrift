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

namespace * ezbake.training

include "EzBakeBase.thrift"

/**
 * A word within a Tweet.
 */
struct TweetWord {
    /**
     * The word within the Tweet.
     */
    1: required string word;
}

/*
 * Thrift version of some fields from the Tweet JSON from Twitter.
 */
struct Tweet {
    /**
     * Tweet ID.
     */
    1: required i64 id;

    /**
     * Text/contents of the Tweet.
     */
    2: required string text;

    /**
     * User name of the sender.
     */
    3: required string userName;

    /**
     * User ID of the sender.
     */
    4: required i64 userId

    /**
     * Timestamp (seconds since UNIX epoch) that that Tweet was sent.
     */
    5: required i64 timestamp;

    /**
     * If this Tweet is a favorite.
     */
    6: required bool isFavorite;

    /**
     * If this Tweet is a re-tweet of another.
     */
    7: required bool isRetweet;

    /*
     * Geographical coordinate from which the Tweet was sent.
     */
    8: optional EzBakeBase.Coordinate geoLocation;
}

/**
 * Service name used to contact the word count service.
 */
const string WORD_COUNT_SERVICE_NAME = "tweetwordcountservice";

/**
 * A service which accumulates the number of distinct words in Tweets.
 */
service TweetWordCountService extends EzBakeBase.EzBakeBaseService {
    /**
     * Adds a instance of a word. The count for that word will be incremented.
     *
     * @param word Word to add
     * @param securityToken EzBake security token
     */
    void add(1: string word, 2: EzBakeBase.EzSecurityToken securityToken);

    /**
     * Gets how many times the given word has been encountered across Tweets.
     *
     * @param word Word whose count to query
     * @param securityToken EzBake security token
     *
     * @returns count of the requested word
     */
    i32 getCount(1: string word, 2: EzBakeBase.EzSecurityToken securityToken);
}