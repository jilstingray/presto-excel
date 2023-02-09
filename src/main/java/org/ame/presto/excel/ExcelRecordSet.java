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
import com.facebook.presto.spi.RecordSet;

import java.io.File;
import java.util.List;

public class ExcelRecordSet
        implements RecordSet
{
    private File file;

    public ExcelRecordSet(File file)
    {
        this.file = file;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ExcelTableColumnUtils.tableColumnTypes(file.toPath());
    }

    @Override
    public RecordCursor cursor()
    {
        return new ExcelRecordCursor(file);
    }
}
