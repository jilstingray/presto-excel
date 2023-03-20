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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.monitorjbl.xlsx.StreamingReader;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.ame.presto.excel.session.ISession;
import org.ame.presto.excel.session.SessionProvider;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.ame.presto.excel.FileTypeJudge.isXlsxFile;

public class ExcelRecordCursor
        implements RecordCursor
{
    private final List<ExcelColumnHandle> columnHandles;
    private Integer rowCacheSize;
    private Integer bufferSize;
    private final Long totalBytes;
    private List<String> fields;
    private final ISession session;
    private InputStream inputStream;
    private Workbook workbook;
    private Sheet sheet;
    private Iterator<Row> iterator;

    public ExcelRecordCursor(List<ExcelColumnHandle> columnHandles, SchemaTableName schemaTableName, Map<String, String> sessionInfo)
            throws Exception
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        rowCacheSize = Integer.parseInt(sessionInfo.get("rowCacheSize"));
        bufferSize = Integer.parseInt(sessionInfo.get("bufferSize"));
        session = new SessionProvider(sessionInfo).getSession();
        inputStream = session.getInputStream(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        totalBytes = (long) inputStream.available();
        // use streaming reader for xlsx files
        if (isXlsxFile(schemaTableName.getTableName())) {
            workbook = StreamingReader.builder().rowCacheSize(rowCacheSize).bufferSize(bufferSize).open(inputStream);
        }
        else {
            workbook = WorkbookFactory.create(inputStream);
        }
        sheet = workbook.getSheetAt(0);
        iterator = sheet.rowIterator();
        // Assume the first row is always the header
        iterator.next();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!iterator.hasNext()) {
            return false;
        }
        Row row = iterator.next();
        // skip empty rows
        if (row == null) {
            return true;
        }
        String[] allFields = new String[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            int ordinalPosition = columnHandles.get(i).getOrdinalPosition();
            Cell cell = row.getCell(ordinalPosition);
            // populate incomplete columns with null
            if (cell == null) {
                allFields[i] = null;
                continue;
            }
            // convert cell to string
            switch (cell.getCellType()) {
                case STRING:
                    allFields[i] = cell.getStringCellValue();
                    break;
                case NUMERIC: //数字
                    if (DateUtil.isCellDateFormatted(cell)) {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        allFields[i] = format.format(cell.getDateCellValue());
                    }
                    else {
                        // prevent integer from being converted to double
                        Long longValue = Math.round(cell.getNumericCellValue());
                        Double doubleValue = cell.getNumericCellValue();
                        if (Double.parseDouble(longValue + ".0") == doubleValue) {
                            allFields[i] = longValue.toString();
                        }
                        else {
                            // avoid scientific notation
                            BigDecimal bigDecimal = BigDecimal.valueOf(cell.getNumericCellValue());
                            allFields[i] = bigDecimal.toPlainString();
                        }
                    }
                    break;
                case BOOLEAN:
                    allFields[i] = Boolean.toString(cell.getBooleanCellValue());
                    break;
                case BLANK:
                    allFields[i] = null;
                    break;
                case FORMULA:
                    allFields[i] = cell.getCellFormula();
                    break;
                default:
                    break;
            }
        }
        fields = Arrays.asList(allFields);
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(fields.get(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    @Override
    public void close()
    {
        try {
            workbook.close();
            inputStream.close();
            session.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }
}
