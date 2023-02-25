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
package org.ame.presto.excel.protocol;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class SFTPSession
        implements ISession
{
    private static final Integer TIMEOUT = 10000;
    private String host;
    private int port;
    private String username;
    private String password;
    private ChannelSftp channel;

    public SFTPSession(String host, int port, String username, String password)
            throws JSchException
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        JSch jsch = new JSch();
        com.jcraft.jsch.Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(TIMEOUT);
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
    }

    public InputStream getInputStream(String path)
            throws SftpException
    {
        InputStream inputStream = channel.get(path);
        return inputStream;
    }

    public List<String> getSchemas(String path)
            throws SftpException
    {
        List<String> schemas = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(path);
        for (ChannelSftp.LsEntry entry : entries) {
            if (entry.getAttrs().isDir()) {
                schemas.add(entry.getFilename());
            }
        }
        return schemas;
    }

    public List<String> getTables(String path, String schema)
            throws SftpException
    {
        List<String> tables = new ArrayList<>();
        List<ChannelSftp.LsEntry> entries = channel.ls(path + "/" + schema);
        for (ChannelSftp.LsEntry entry : entries) {
            if (!entry.getAttrs().isDir() && entry.getFilename().endsWith(".xlsx")) {
                tables.add(entry.getFilename().replace(".xlsx", ""));
            }
        }
        return tables;
    }

    public void close()
            throws JSchException
    {
        channel.disconnect();
        channel.getSession().disconnect();
    }
}
