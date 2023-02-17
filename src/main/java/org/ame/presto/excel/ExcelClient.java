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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();

    @Inject
    public ExcelClient(ExcelConfig config, JsonCodec<Map<String, List<ExcelTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        this.config = config;
    }

    public ExcelConfig getConfig()
    {
        return config;
    }

    public Optional<ExcelTable> getTable(String schemaName, String tableName)
    {
        List<List<Object>> values = readAllValues(schemaName, tableName);
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

    private List<List<Object>> readAllValues(String schemaName, String tableName)
    {
        try {
            Path path = config.getBaseDir().toPath().resolve(schemaName).resolve(tableName + ".xlsx");
            Workbook workbook = WorkbookFactory.create(path.toFile());
            // TODO: support multiple sheets
            Sheet sheet = workbook.getSheetAt(0);
            List<List<Object>> values = new ArrayList<>();
            for (Row row : sheet) {
                List<Object> value = new ArrayList<>();
                for (int i = 0; i < row.getLastCellNum(); i++) {
                    Cell cell = row.getCell(i);
                    if (cell.equals(CellType.NUMERIC)) {
                        if (DateUtil.isCellDateFormatted(cell)) {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            value.add(format.format(cell.getDateCellValue()));
                        }
                        else {
                            // avoid scientific notation
                            BigDecimal bigDecimal = new BigDecimal(cell.getNumericCellValue());
                            value.add(bigDecimal.toPlainString());
                        }
                    }
                    else {
                        value.add(DATA_FORMATTER.formatCellValue(cell));
                    }
                }
                values.add(value);
            }
            workbook.close();
            return values;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
