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
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExcelRecordCursor
        implements RecordCursor
{
    private final File file;
    private List<String> fields;
    private Workbook workbook;
    private Sheet sheet;
    private Iterator<Row> iterator;
    private boolean hasNext;
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();

    public ExcelRecordCursor(File file)
    {
        this.file = file;
        this.workbook = null;
        this.sheet = null;
        this.iterator = null;
        this.hasNext = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return VarcharType.VARCHAR;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (sheet == null) {
            try {
                fields = new ArrayList<>();
                InputStream inputStream = new FileInputStream(file);
                workbook = WorkbookFactory.create(inputStream);
                // TODO: support multiple sheets
                sheet = workbook.getSheetAt(0);
                iterator = sheet.rowIterator();
                // skip header
                iterator.next();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            // TODO: skip empty rows in the middle
            hasNext = iterator.hasNext();
            if (hasNext) {
                Row row = iterator.next();
                fields = new ArrayList<>();
                for (Cell cell : row) {
                    fields.add(DATA_FORMATTER.formatCellValue(cell));
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return hasNext;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice(fields.get(field));
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        if (workbook != null) {
            try {
                sheet = null;
                iterator = null;
                workbook.close();
            }
            catch (IOException e) {
//                throw new RuntimeException(e);
            }
        }
    }
}
