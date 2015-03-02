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

import ezbake.frack.common.workers.IThriftConverter;
import ezbake.thrift.ThriftUtils;
import ezbake.warehaus.Repository;

public class RepositoryConverter implements IThriftConverter<TweetWithRaw, Repository> {
    private static final long serialVersionUID = 1L;

    private String uriPrefix = null;

    public void setUriPrefix(String uriPrefix) {
        this.uriPrefix = uriPrefix;
    }

    @Override
    public Repository convert(TweetWithRaw tweetAndRaw) throws TException {
        Repository repo = new Repository();
        repo.setParsedData(ThriftUtils.serialize(tweetAndRaw.getTweet()));
        if (tweetAndRaw.getRaw() != null) {
            repo.setRawData(tweetAndRaw.getRaw().getBytes());
        }
        repo.setUri(uriPrefix + tweetAndRaw.getTweet().getId());
        repo.setUpdateVisibility(false);
        return repo;
    }
}
