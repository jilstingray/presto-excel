# Presto Excel Connector 

Based on [presto-localcsv](https://github.com/dongqianwei/presto-localcsv), this connector allows Presto to query data stored in local Excel files.

Currently, the connector only supports Excel 2007+ format (*.xlsx) with a single sheet.

## Build

*(WIP)*

## Configuration

Create a catalog properties file `excel.properties` in etc/catalog.

```
connector.name=excel
excel.base=/data/exceldb
```

`excel.base` sets the root directory, schema name is the second level directory, and table name is the Excel file name without suffix.

## TODO

- [ ] Support old Excel format (*.xls)

- [ ] Auto conversion of special data types (e.g. date, time, datetime)

- [ ] Support full Excel manipulation (e.g. create table, insert, delete, update)

- [ ] Merge with `presto-localcsv` into a single connector supporting multiple file formats



