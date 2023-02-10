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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.google.inject.Inject;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExcelUtils
{
    private ExcelConfig config;

    @Inject
    private ExcelUtils(ExcelConfig config)
    {
        this.config = config;
    }

    public static List<Type> tableColumnTypes(Path path)
    {
        return tableColumns(path).stream().map(type -> VarcharType.VARCHAR).collect(Collectors.toList());
    }

    public List<String> tableColumns(String schemaName, String tableName)
    {
        Path filePath = config.getBaseDir().toPath().resolve(schemaName).resolve(tableName + ".xlsx");
        return tableColumns(filePath);
    }

    public static List<String> tableColumns(Path path)
    {
        try {
            Workbook workbook = WorkbookFactory.create(path.toFile());
            Sheet sheet = workbook.getSheetAt(0);
            Row row = sheet.getRow(0);
            List<String> columns = new ArrayList<>();
            for (int i = 0; i < row.getLastCellNum(); i++) {
                columns.add(row.getCell(i).getStringCellValue());
            }
            return columns;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
