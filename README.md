# Presto Excel Connector

This connector allows Presto to query data stored in Excel files from local or SFTP storage.

Currently, the connector supports `select` on the first sheets of `.xls`, `.xlsx` files.

## Compile

Download the source code of Presto from [GitHub](https://github.com/prestodb/presto/), copy `presto-excel` into it, and add the following line to `pom.xml` under the root module:

```xml
<module>presto-excel</module>
```

## Configuration

Create a catalog properties file `etc/catalog/excel.properties`. To read files from local drives, add something like this:

```
connector.name=excel
excel.protocol=file
excel.base=/path/to/dir
```

`excel.base` sets the root directory. The schema name is the second level directory, and table name is the Excel filename.

The connector also supports reading files from a SFTP server.

```
connector.name=excel
excel.protocol=sftp
excel.base=/path/to/dir
excel.host=xxx.xxx.xxx.xxx
excel.port=xxx
excel.username=xxx
excel.password=xxx
```

Excel Connector uses [Excel Streaming Reader](https://github.com/monitorjbl/excel-streaming-reader) to load large `.xlsx` files. `rowCacheSize` and `bufferSize` can be
adjusted in the configuration file, and the default values are 100 and 4096.

```
excel.xlsx-row-cache-size=100   # number of rows to keep in memory
excel.xlsx-buffer-size=4096     # buffer size to use when reading InputStream to file
```

## Known issues

Presto does not support uppercase table names (see [this issue](https://github.com/prestodb/presto/issues/2863)). The connector can not recognize the file if its name contains
uppercase letters either.

## TODO

- [ ] Support multiple sheets.

- [ ] Support HDFS, HTTP Server, etc.