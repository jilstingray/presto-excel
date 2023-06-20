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
package com.facebook.presto.excel;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.excel.session.ISession;
import com.facebook.presto.excel.session.SessionProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExcelClient
{
    private final Logger logger = Logger.get(ExcelClient.class);
    private final ExcelConfig config;
    private static Integer rowCacheSize = 100;
    private static Integer bufferSize = 4096;

    @Inject
    public ExcelClient(ExcelConfig config, JsonCodec<Map<String, List<ExcelTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
        if (config.getRowCacheSize() != null) {
            rowCacheSize = config.getRowCacheSize();
        }
        if (config.getBufferSize() != null) {
            bufferSize = config.getBufferSize();
        }
    }

    public Optional<ExcelTable> getTable(String schemaName, String tableName)
    {
        ImmutableList.Builder<ExcelColumn> columns = ImmutableList.builder();
        // Assume the first row is always the header
        List<Object> header = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        try {
            ISession session = getSession();
            InputStream inputStream = session.getInputStream(schemaName, tableName);
            Workbook workbook;
            // use streaming reader for xlsx files
            if (FileTypeJudge.isXlsxFile(tableName)) {
                workbook = StreamingReader.builder().bufferSize(bufferSize).open(inputStream);
            }
            else {
                workbook = WorkbookFactory.create(inputStream);
            }
            Sheet sheet = workbook.getSheetAt(0);
            // sheet.getRow() is not supported in excel-streaming-reader
            Row row = sheet.rowIterator().next();
            for (Cell cell : row) {
                // skip empty cells
                if (cell == null || cell.getStringCellValue().isEmpty()) {
                    continue;
                }
                header.add(cell.getStringCellValue());
            }
            workbook.close();
            inputStream.close();
            session.close();
        }
        catch (Exception e) {
            logger.warn(e, "Error while reading excel file %s", tableName);
            return Optional.empty();
        }
        for (int i = 0; i < header.size(); i++) {
            String columnName = header.get(i).toString().toLowerCase(Locale.ENGLISH);
            // when empty or repeated column header, adding a placeholder column name
            if (columnName.isEmpty() || columnNames.contains(columnName)) {
                columnName = "column_" + i;
            }
            columnNames.add(columnName);
            columns.add(new ExcelColumn(columnName, VarcharType.VARCHAR));
        }
        return Optional.of(new ExcelTable(tableName, columns.build()));
    }

    public List<String> getSchemaNames()
    {
        try {
            ISession session = getSession();
            List<String> schemas = session.getSchemas();
            session.close();
            return ImmutableList.copyOf(schemas);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getTableNames(String schemaName)
    {
        try {
            ISession session = getSession();
            List<String> tables = session.getTables(schemaName);
            session.close();
            return ImmutableList.copyOf(tables);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ISession getSession()
    {
        Map<String, String> sessionInfo = new HashMap<>();
        sessionInfo.put("base", config.getBase());
        sessionInfo.put("protocol", config.getProtocol());
        sessionInfo.put("host", config.getHost());
        sessionInfo.put("port", config.getPort().toString());
        sessionInfo.put("username", config.getUsername());
        sessionInfo.put("password", config.getPassword());
        sessionInfo.put("rowCacheSize", rowCacheSize.toString());
        sessionInfo.put("bufferSize", bufferSize.toString());
        return new SessionProvider(sessionInfo).getSession();
    }

    public Integer getRowCacheSize()
    {
        return rowCacheSize;
    }

    public Integer getBufferSize()
    {
        return bufferSize;
    }
}
