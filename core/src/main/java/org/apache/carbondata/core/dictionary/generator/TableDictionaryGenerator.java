/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.dictionary.generator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

/**
 * Dictionary generation for table.
 */
public class TableDictionaryGenerator
    implements DictionaryGenerator<Integer, DictionaryKey>, DictionaryWriter {

  private Map<String, DictionaryGenerator<Integer, String>> columnMap = new ConcurrentHashMap<>();

  public TableDictionaryGenerator(CarbonDimension dimension) {
    columnMap.put(dimension.getColumnId(),
            new IncrementalColumnDictionaryGenerator(dimension, 1));
  }

  @Override public Integer generateKey(DictionaryKey value) throws DictionaryGenerationException {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(value.getTableUniqueName());
    CarbonDimension dimension = carbonTable.getAllDimensionByName(
            value.getTableUniqueName(), value.getColumnName());

    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return generator.generateKey(value.getData().toString());
  }

  public Integer size(DictionaryKey key) {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(key.getTableUniqueName());
    CarbonDimension dimension = carbonTable.getAllDimensionByName(
            key.getTableUniqueName(), key.getColumnName());

    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return ((BiDictionary) generator).size();
  }

  @Override public void writeDictionaryData(DictionaryKey key) throws Exception {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(key.getTableUniqueName());
    CarbonDimension dimension = carbonTable.getAllDimensionByName(
            key.getTableUniqueName(), key.getColumnName());

    DictionaryGenerator generator = columnMap.get(dimension.getColumnId());
    ((DictionaryWriter) (generator)).writeDictionaryData(key);
  }

  public void updateGenerator(CarbonDimension dimension) {
    columnMap.put(dimension.getColumnId(),
            new IncrementalColumnDictionaryGenerator(dimension, 1));
  }
}
