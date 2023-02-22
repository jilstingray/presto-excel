# Presto Excel Connector 

Based on [presto-localcsv](https://github.com/dongqianwei/presto-localcsv) and [presto-google-sheets](https://github.com/prestodb/presto/tree/master/presto-google-sheets), this connector allows Presto to query data stored in local Excel files.

Currently, the connector supports `select` on Excel 2007+ format files (`*.xlsx`) from local drives/SFTP server.

## Compile

Download the source code of Presto from [GitHub](https://github.com/prestodb/presto/), copy `presto-excel` into it, and add the following line to `pom.xml` under `presto-root` module:

```xml
<module>presto-excel</module>
```

Build Presto.

## Configuration

Create a catalog properties file `etc/catalog/excel.properties`. If you want to read files from your local drives, add something like this:

```
connector.name=excel
excel.protocol=file
excel.base=/path/to/directory
```

`excel.base` sets the root directory. The schema name is the second level directory, and table name is the Excel file name without suffix.

The connector also supports reading files from a SFTP server.

```
connector.name=excel
excel.protocol=sftp
excel.base=/path/to/directory
excel.host=xxx.xxx.xxx.xxx
excel.port=xxx
excel.username=xxx
excel.password=xxx
```

## TODO

- [x] Support SFTP.

- [ ] Support HDFS, HTTP Server, etc.

- [ ] Support full capabilities of Excel manipulation (`insert`, `delete`, `update`, `create table`, `drop table`, etc).

- [ ] Support more file formats, not just `*.xlsx`.