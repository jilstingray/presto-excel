# Presto Excel Connector 

Presto 本地 Excel 连接器，参考了 [presto-localcsv](https://github.com/dongqianwei/presto-localcsv) 和 [presto-google-sheets](https://github.com/prestodb/presto/tree/master/presto-google-sheets)。

目前支持查询 Excel 2007+ 格式（*.xlsx）的工作表。

## 编译 & 构建

*（待补充）*

## 配置

创建配置文件 `etc/catalog/excel.properties`：

```
connector.name=excel
excel.base=/data/exceldb
```

`excel.base` 为根目录，schema 名称对应第二级目录，table 名称对应 Excel 文件名（不包含后缀）。

## TODO

- [ ] 支持旧的 Excel 格式（*.xls）
- 
- [ ] 支持多张工作表（sheet)

- [ ] 自动转换特殊数据类型 (e.g. date, time, datetime)

- [ ] 支持完整的 Excel 操作（e.g. create table, insert, delete, update）

- [ ] 合并为支持多种文件格式的单一连接器



