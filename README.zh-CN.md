# Presto Excel Connector

Presto Excel 连接器，目前支持查询本地或 SFTP 服务器上的 `.xls`、`.xlsx` 文件的单张 sheet。

## 编译

从 [GitHub](https://github.com/prestodb/presto/) 获取 Presto 源码，将本项目复制进去，在根目录的 `pom.xml` 中添加模块：

```xml
<module>presto-excel</module>
```

## 配置

创建配置文件 `etc/catalog/excel.properties`。读取本地文件的配置如下：

```
connector.name=excel
excel.protocol=file
excel.base=/path/to/directory
```

`excel.base` 为根目录，schema 名称对应第二级目录，table 名称对应 Excel 文件名（不包含后缀）。

也可以从 SFTP 服务器读取文件：

```
connector.name=excel
excel.protocol=sftp
excel.base=/path/to/directory
excel.host=xxx.xxx.xxx.xxx
excel.port=xxx
excel.username=xxx
excel.password=xxx
```

连接器使用了 [Excel Streaming Reader](https://github.com/monitorjbl/excel-streaming-reader) 实现超大文件的读取，读取时使用的 `rowCacheSize` 和 `bufferSize` 可以在配置文件中调整，默认值为
100 和 4096。

```
excel.xlsx-row-cache-size=100   # number of rows to keep in memory
excel.xlsx-buffer-size=4096     # buffer size to use when reading InputStream to file
```

## 已知问题

Presto 不支持大写表名（见 [这个 issue](https://github.com/prestodb/presto/issues/2863)），该连接器也不支持文件名中包含大写字母的文件。

## TODO

- [ ] 支持多张工作表。

- [ ] 支持 HDFS、HTTP 等协议。
