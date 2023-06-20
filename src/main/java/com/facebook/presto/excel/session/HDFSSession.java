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

import com.facebook.presto.excel.FileTypeJudge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HDFSSession
        implements ISession
{
    private final FileSystem fileSystem;
    private String base;

    public HDFSSession(Map<String, String> sessionInfo)
            throws Exception
    {
        base = sessionInfo.get("base");
        if(!base.startsWith("/")){
            base = "/" + base;
        }
        if (!base.endsWith("/")) {
            base += "/";
        }
        String host = sessionInfo.get("host");
        String port = sessionInfo.get("port");
        String url = "hdfs://" + host + ":" + port;
        String username = sessionInfo.get("username");
        fileSystem = FileSystem.newInstance(new URI(url), new Configuration(), username);
    }

    @Override
    public InputStream getInputStream(String schemaName, String tableName)
            throws Exception
    {
        return fileSystem.open(new org.apache.hadoop.fs.Path(base + schemaName + "/" + tableName));
    }

    @Override
    public List<String> getSchemas()
            throws Exception
    {
        List<String> schemas = new ArrayList<>();
        for (org.apache.hadoop.fs.FileStatus fileStatus : fileSystem.listStatus(new org.apache.hadoop.fs.Path(base))) {
            if (fileStatus.isDirectory()) {
                schemas.add(fileStatus.getPath().getName());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTables(String schemaName)
            throws Exception
    {
        List<String> tables = new ArrayList<>();
        for (org.apache.hadoop.fs.FileStatus fileStatus : fileSystem.listStatus(new org.apache.hadoop.fs.Path(base + schemaName))) {
            if (fileStatus.isFile() && FileTypeJudge.isExcelFile(fileStatus.getPath().getName())) {
                tables.add(fileStatus.getPath().getName());
            }
        }
        return tables;
    }

    @Override
    public void close()
    {
        try {
            fileSystem.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
