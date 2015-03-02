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

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import ezbake.base.thrift.Coordinate;
import ezbake.base.thrift.SSR;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.TimeUtil;
import ezbake.frack.common.utils.thrift.SSRJSON;
import ezbake.frack.common.workers.IThriftConverter;

public class SSRConverter implements IThriftConverter<TweetWithRaw, SSRJSON> {
    private static final long serialVersionUID = 1L;

    private String uriPrefix = null;

    public void setUriPrefix(String uriPrefix) {
        this.uriPrefix = uriPrefix;
    }

    @Override
    public SSRJSON convert(TweetWithRaw tweetAndRaw) throws TException {
        Tweet tweet = tweetAndRaw.getTweet();
        SSRJSON ssrJson = new SSRJSON();
        SSR ssr = new SSR();
        ssr.setUri(uriPrefix + tweet.getId());
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
        ssrJson.setJsonString(new TSerializer(new TSimpleJSONProtocol.Factory()).toString(tweet));
        return ssrJson;
    }
}
