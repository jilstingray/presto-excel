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

import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ExcelMetadata
        implements ConnectorMetadata
{
    private ExcelConfig config;
    private ExcelUtils utils;

    @Inject
    public ExcelMetadata(ExcelConfig config, ExcelUtils utils)
    {
        this.config = config;
        this.utils = utils;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return Files.list(config.getBaseDir().toPath())
                    .filter(Files::isDirectory)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExcelTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Path tablePath = config.getBaseDir().toPath().resolve(tableName.getSchemaName());
        if (!Files.exists(tablePath) || !Files.isDirectory(tablePath)) {
            return null;
        }
        Path filePath = tablePath.resolve(tableName.getTableName() + ".xlsx");
        if (!Files.exists(filePath)) {
            return null;
        }
        return new ExcelTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ExcelTableHandle tableHandle = (ExcelTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ExcelTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ExcelTableHandle excelTableHandle = (ExcelTableHandle) table;
        List<String> columns = utils.tableColumns(excelTableHandle.getSchemaName(), excelTableHandle.getTableName());
        List<ColumnMetadata> metadata = columns.stream()
                .map(column -> new ColumnMetadata(column, VarcharType.VARCHAR))
                .collect(Collectors.toList());
        return new ConnectorTableMetadata(excelTableHandle.toSchemaTableName(), metadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<String> schemaListBuilder = ImmutableList.builder();
        ImmutableList.Builder<SchemaTableName> tableListBuilder = ImmutableList.builder();
        if (schemaName.isPresent()) {
            schemaListBuilder.add(schemaName.get());
        }
        else {
            schemaListBuilder.addAll(listSchemaNames(session));
        }
        ImmutableList<String> schemaList = schemaListBuilder.build();
        for (String schema : schemaList) {
            tableListBuilder.addAll(listTables(schema));
        }
        return tableListBuilder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        ExcelTableHandle handle = (ExcelTableHandle) tableHandle;
        List<String> columns = utils.tableColumns(handle.getSchemaName(), handle.getTableName());
        for (int i = 0; i < columns.size(); i++) {
            builder.put(columns.get(i), new ExcelColumnHandle(columns.get(i), VarcharType.VARCHAR, i));
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ExcelColumnHandle handle = (ExcelColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getColumnName(), handle.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> list = new ArrayList<>();
        if (prefix.getTableName() != null) {
            list.add(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            list.addAll(listTables(prefix.getSchemaName()));
        }
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (SchemaTableName schemaTableName : list) {
            List<String> columns = utils.tableColumns(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            List<ColumnMetadata> metadata = columns.stream()
                    .map(column -> new ColumnMetadata(column, VarcharType.VARCHAR))
                    .collect(Collectors.toList());
            builder.put(schemaTableName, metadata);
        }
        return builder.build();
    }

    private List<SchemaTableName> listTables(String schemaName)
    {
        try {
            return Files.list(config.getBaseDir().toPath().resolve(schemaName))
                    .map(path -> path.getFileName().toString().replaceAll("\\.xlsx$", ""))
                    .map(tableName -> new SchemaTableName(schemaName, tableName))
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
