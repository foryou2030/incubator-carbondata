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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
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

  public TableDictionaryGenerator(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.hasEncoding(Encoding.DICTIONARY) && !dimension
          .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        columnMap
            .put(dimension.getColumnId(), new IncrementalColumnDictionaryGenerator(dimension, 1));
      }
    }
  }

  @Override public Integer generateKey(DictionaryKey value) throws DictionaryGenerationException {
    DictionaryGenerator<Integer, String> generator =
            columnMap.get(value.getColumnIdentifier().getColumnId());
    return generator.generateKey(value.getData().toString());
  }

  public Integer size(DictionaryKey key) {
    DictionaryGenerator<Integer, String> generator =
            columnMap.get(key.getColumnIdentifier().getColumnId());
    return ((BiDictionary) generator).size();
  }

  @Override public void writeDictionaryData(DictionaryKey key) throws IOException {
    for (DictionaryGenerator generator : columnMap.values()) {
      ((DictionaryWriter) (generator)).writeDictionaryData(key);
    }
  }
}
