# Presto Excel Connector 

Based on [presto-localcsv](https://github.com/dongqianwei/presto-localcsv) and [presto-google-sheets](https://github.com/prestodb/presto/tree/master/presto-google-sheets), this connector allows Presto to query data stored in local Excel files.

Currently, the connector supports `select` on Excel 2007+ format files (*.xlsx).

## Build

* (to be added) *

## Configuration

Create a catalog properties file `excel.properties` in etc/catalog.

```
connector.name=excel
excel.base=/data/exceldb
```

`excel.base` sets the root directory, schema name is the second level directory, and table name is the Excel file name without suffix.

## TODO

- [ ] Support old Excel format (*.xls)

- [ ] Support multiple sheets

- [ ] Auto conversion of special data types (e.g. date, time, datetime)

- [ ] Support full Excel manipulation (e.g. create table, insert, delete, update)

- [ ] Support more file formats (not just Excel)



