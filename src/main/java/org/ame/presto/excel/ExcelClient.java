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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.ame.presto.excel.protocol.ISession;
import org.ame.presto.excel.protocol.SessionProvider;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
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
    private final ExcelConfig config;
    private ISession session;
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();
    private static final Integer MAX_ROWS = 99999;

    @Inject
    public ExcelClient(ExcelConfig config, JsonCodec<Map<String, List<ExcelTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
    }

    public Optional<ExcelTable> getTable(String schemaName, String tableName)
    {
        List<List<Object>> values;
        try {
            values = readAllValues(schemaName, tableName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (values.size() == 0) {
            return Optional.empty();
        }
        ImmutableList.Builder<ExcelColumn> columns = ImmutableList.builder();
        // Assuming 1st line is always header
        List<Object> header = values.get(0);
        Set<String> columnNames = new HashSet<>();
        for (int i = 0; i < header.size(); i++) {
            String columnName = header.get(i).toString().toLowerCase(Locale.ENGLISH);
            // when empty or repeated column header, adding a placeholder column name
            if (columnName.isEmpty() || columnNames.contains(columnName)) {
                columnName = "column_" + i;
            }
            columnNames.add(columnName);
            columns.add(new ExcelColumn(columnName, VarcharType.VARCHAR));
        }
        List<List<Object>> data = values.subList(1, values.size());
        return Optional.of(new ExcelTable(tableName, columns.build(), data));
    }

    // TODO: optimize memory usage
    private List<List<Object>> readAllValues(String schemaName, String tableName)
            throws Exception
    {
        session = new SessionProvider(getSessionInfo()).getSession();
        InputStream inputStream = session.getInputStream(schemaName, tableName);
        Workbook workbook = WorkbookFactory.create(inputStream);
        Sheet sheet = workbook.getSheetAt(0);
        List<List<Object>> values = new ArrayList<>();
        for (Row row : sheet) {
            if (row == null) {
                continue;
            }
            // FAILSAFE: limit memory usage
            if (values.size() > MAX_ROWS) {
                break;
            }
            List<Object> value = new ArrayList<>();
            for (Cell cell : row) {
                if (cell == null) {
                    continue;
                }
                if (cell.getCellType().equals(CellType.NUMERIC)) {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        value.add(format.format(cell.getDateCellValue()));
                    }
                    else {
                        // prevent integer from being converted to double
                        Long longValue = Math.round(cell.getNumericCellValue());
                        Double doubleValue = cell.getNumericCellValue();
                        if (Double.parseDouble(longValue + ".0") == doubleValue) {
                            value.add(longValue.toString());
                        }
                        else {
                            // avoid scientific notation
                            BigDecimal bigDecimal = BigDecimal.valueOf(cell.getNumericCellValue());
                            value.add(bigDecimal.toPlainString());
                        }
                    }
                }
                else {
                    value.add(DATA_FORMATTER.formatCellValue(cell));
                }
            }
            values.add(value);
        }
        workbook.close();
        inputStream.close();
        session.close();
        return values;
    }

    public List<String> getSchemaNames()
    {
        try {
            session = new SessionProvider(getSessionInfo()).getSession();
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
            session = new SessionProvider(getSessionInfo()).getSession();
            List<String> tables = session.getTables(schemaName);
            session.close();
            return ImmutableList.copyOf(tables);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getSessionInfo()
    {
        Map<String, String> sessionInfo = new HashMap<>();
        sessionInfo.put("base", config.getBase());
        sessionInfo.put("protocol", config.getProtocol());
        sessionInfo.put("host", config.getHost());
        sessionInfo.put("port", config.getPort().toString());
        sessionInfo.put("username", config.getUsername());
        sessionInfo.put("password", config.getPassword());
        return sessionInfo;
    }
}
