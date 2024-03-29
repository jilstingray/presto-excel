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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class ExcelTable
{
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public ExcelTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ExcelColumn> columns)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        requireNonNull(columns, "columns is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadataBuilder = ImmutableList.builder();
        for (ExcelColumn column : columns) {
            columnsMetadataBuilder.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadataBuilder.build();
    }

    @JsonProperty
    public String getName()
    {
        return columnsMetadata.get(0).getName();
    }

    @JsonProperty
    public List<ExcelColumn> getColumns()
    {
        ImmutableList.Builder<ExcelColumn> columnsBuilder = ImmutableList.builder();
        for (ColumnMetadata columnMetadata : columnsMetadata) {
            columnsBuilder.add(new ExcelColumn(columnMetadata.getName(), columnMetadata.getType()));
        }
        return columnsBuilder.build();
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
