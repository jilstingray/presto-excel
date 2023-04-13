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
package org.ame.presto.excel.session;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.ame.presto.excel.FileTypeJudge.isExcelFile;

public class SFTPSession
        implements ISession
{
    private static final Integer TIMEOUT = 10000;
    private final Session session;
    private final ChannelSftp channel;
    private String base;

    public SFTPSession(Map<String, String> sessionInfo)
            throws Exception
    {
        this.base = sessionInfo.get("base");
        if (base.endsWith("/") || base.endsWith("\\")) {
            base = base.substring(0, base.length() - 1);
        }
        String host = sessionInfo.get("host");
        int port = Integer.parseInt(sessionInfo.get("port"));
        String username = sessionInfo.get("username");
        String password = sessionInfo.get("password");
        session = new JSch().getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(TIMEOUT);
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect(TIMEOUT);
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws Exception
    {
        return channel.get(base + "/" + schemaName + "/" + tableName);
    }

    @Override
    public List<String> getSchemas()
            throws Exception
    {
        List<String> schemas = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(base);
        for (ChannelSftp.LsEntry entry : entries) {
            if (entry.getAttrs().isDir()) {
                schemas.add(entry.getFilename());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTables(String schemaName)
            throws Exception
    {
        List<String> tables = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(base + "/" + schemaName);
        for (ChannelSftp.LsEntry entry : entries) {
            if (!entry.getAttrs().isDir() && isExcelFile(entry.getFilename())) {
                tables.add(entry.getFilename());
            }
        }
        return tables;
    }

    @Override
    public void close()
    {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }
}
