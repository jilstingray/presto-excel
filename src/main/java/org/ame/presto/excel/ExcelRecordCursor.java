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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExcelRecordCursor
        implements RecordCursor
{
    private final List<ExcelColumnHandle> columnHandles;
    private final Long totalBytes;
    private List<String> fields;
    private final List<List<Object>> dataValues;
    private Integer currentIndex;

    public ExcelRecordCursor(List<ExcelColumnHandle> columnHandles, List<List<Object>> dataValues)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(dataValues, "dataValues is null");

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.dataValues = ImmutableList.copyOf(dataValues);
        long inputLength = 0;
        for (List<Object> objList : dataValues) {
            for (Object obj : objList) {
                inputLength += String.valueOf(obj).length();
            }
        }
        totalBytes = inputLength;
        this.currentIndex = 0;
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
        List<Object> currentVals = null;
        // skip empty rows
        while (currentVals == null || currentVals.size() == 0) {
            if (currentIndex == dataValues.size()) {
                return false;
            }
            currentVals = dataValues.get(currentIndex++);
        }
        // populate incomplete columns with null
        String[] allFields = new String[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            int ordinalPosition = columnHandles.get(i).getOrdinalPosition();
            if (currentVals.size() > ordinalPosition) {
                allFields[i] = String.valueOf(currentVals.get(ordinalPosition));
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
