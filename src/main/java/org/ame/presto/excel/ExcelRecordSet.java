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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import org.ame.presto.excel.session.ISession;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ExcelRecordSet
        implements RecordSet
{
    private final Logger logger = Logger.get(ExcelRecordSet.class);
    private final List<ExcelColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final SchemaTableName schemaTableName;
    private ISession session;
    private Integer rowCacheSize;

    private Integer bufferSize;

    public ExcelRecordSet(ExcelSplit split, List<ExcelColumnHandle> columnHandles, ISession session, Integer rowCacheSize, Integer bufferSize)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream().map(ExcelColumnHandle::getColumnType).collect(Collectors.toList());
        requireNonNull(split, "split is null");
        this.schemaTableName = new SchemaTableName(split.getSchemaName(), split.getTableName());
        this.session = session;
        this.rowCacheSize = rowCacheSize;
        this.bufferSize = bufferSize;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        try {
            return new ExcelRecordCursor(columnHandles, schemaTableName, session, rowCacheSize, bufferSize);
        }
        catch (Exception e) {
            logger.error(e, "Error creating ExcelRecordCursor");
            throw new RuntimeException(e);
        }
    }
}
