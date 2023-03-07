# Presto Excel Connector 

Presto Excel 连接器，参考了 [presto-localcsv](https://github.com/dongqianwei/presto-localcsv) 和 [presto-google-sheets](https://github.com/prestodb/presto/tree/master/presto-google-sheets)。

目前支持查询本地/SFTP服务器上的 Excel 工作表。

## 编译

从 [GitHub](https://github.com/prestodb/presto/) 获取 Presto 源码，将本项目源码复制进去，将模块添加到 `pom.xml` 中：

```xml
<module>presto-excel</module>
```

编译 Presto。

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

## TODO

- [x] 支持 SFTP。

- [ ] 支持 HDFS、HTTP 等协议。

- [ ] 支持 `*.xlsx` 以外的更多文件类型。



