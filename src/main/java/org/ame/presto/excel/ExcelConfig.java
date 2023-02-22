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
package org.ame.presto.excel;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class ExcelConfig
{
    private String protocol;
    private String path;
    private String username;
    private String password;
    private String host;
    private int port;

    @NotNull
    public String getProtocol()
    {
        return protocol;
    }

    @NotNull
    public String getPath()
    {
        return path;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    @Config("excel.protocol")
    public ExcelConfig setProtocol(String protocol)
    {
        this.protocol = protocol;
        return this;
    }

    @Config("excel.base")
    public ExcelConfig setPath(String path)
    {
        this.path = path;
        return this;
    }

    @Config("excel.username")
    public ExcelConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("excel.password")
    public ExcelConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("excel.host")
    public ExcelConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("excel.port")
    public ExcelConfig setPort(int port)
    {
        this.port = port;
        return this;
    }
}
