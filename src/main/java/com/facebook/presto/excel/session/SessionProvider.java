/*
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
 * limitations under the License.
 */
package com.facebook.presto.excel.session;

import java.util.Locale;
import java.util.Map;

public class SessionProvider
{
    private ISession session;

    public SessionProvider(Map<String, String> sessionInfo)
    {
        if (ProtocolType.FILE.toString().equals(sessionInfo.get("protocol").toLowerCase(Locale.ENGLISH))) {
            this.session = new LocalSession(sessionInfo);
        }
        if (ProtocolType.SFTP.toString().equals(sessionInfo.get("protocol").toLowerCase(Locale.ENGLISH))) {
            try {
                this.session = new SFTPSession(sessionInfo);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (ProtocolType.HDFS.toString().equals(sessionInfo.get("protocol").toLowerCase(Locale.ENGLISH))) {
            try {
                this.session = new HDFSSession(sessionInfo);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ISession getSession()
    {
        return session;
    }
}
