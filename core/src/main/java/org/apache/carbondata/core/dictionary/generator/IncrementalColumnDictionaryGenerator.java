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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.factory.CarbonCommonFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.service.PathService;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;


/**
 * This generator does not maintain the whole cache of dictionary. It just maintains the cache only
 * for the loading session, so what ever the dictionary values it generates in the loading session
 * it keeps in cache.
 */
public class IncrementalColumnDictionaryGenerator
    implements BiDictionary<Integer, String>, DictionaryGenerator<Integer, String>,
    DictionaryWriter {

  private final Object lock = new Object();

  private Map<String, Integer> incrementalCache = new ConcurrentHashMap<>();

  private Map<Integer, String> reverseIncrementalCache = new ConcurrentHashMap<>();

  private int maxDictionary;

  private CarbonDimension dimension;

  private CarbonDictionaryWriter dictionaryWriter = null;

  public IncrementalColumnDictionaryGenerator(CarbonDimension dimension, int maxValue) {
    this.maxDictionary = maxValue;
    this.dimension = dimension;
  }

  @Override public Integer getOrGenerateKey(String value) throws DictionaryGenerationException {
    Integer dict = getKey(value);
    if (dict == null) {
      dict = generateKey(value);
    }
    return dict;
  }

  @Override public Integer getKey(String value) {
    return incrementalCache.get(value);
  }

  @Override public String getValue(Integer key) {
    throw new UnsupportedOperationException();
  }

  @Override public int size() {
    return maxDictionary;
  }

  @Override public Integer generateKey(String value) throws DictionaryGenerationException {
    synchronized (lock) {
      Integer dict = incrementalCache.get(value);
      if (dict == null) {
        dict = ++maxDictionary;
        incrementalCache.put(value, dict);
        reverseIncrementalCache.put(dict, value);
      }
      return dict;
    }
  }

  @Override public void writeDictionaryData(DictionaryKey key) throws Exception {
    // initialize params
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(key.getTableUniqueName());
    CarbonTableIdentifier tableIdentifier = carbonTable.getCarbonTableIdentifier();
    ColumnIdentifier columnIdentifier = dimension.getColumnIdentifier();
    String storePath = carbonTable.getStorePath();
    DictionaryService dictionaryService = CarbonCommonFactory.getDictionaryService();
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService.getCarbonTablePath(storePath, tableIdentifier);
    // create dictionary cache from dictionary File
    DictionaryColumnUniqueIdentifier identifier =
            new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier,
                    columnIdentifier.getDataType());
    Boolean isDictExists = isDictionaryExists(identifier, carbonTablePath);
    Dictionary dictionary = null;
    if (isDictExists) {
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache = CacheProvider.getInstance()
              .createCache(CacheType.REVERSE_DICTIONARY, storePath);
      dictionary = dictCache.get(identifier);
    }

    // write dictionary
    List<String> distinctValues = writeDictionary(dictionary,
            dictionaryService, identifier, storePath, isDictExists);
    // write sort index
    writeSortIndex(distinctValues, dictionary,
            dictionaryService, tableIdentifier, columnIdentifier, storePath);
    // update Meta Data
    updateMetaData();
  }

  /**
   * write dictionary to file
   *
   * @param dictionary
   * @param dictionaryService
   * @param dictIdentifier
   * @param storePath
   * @return
   * @throws IOException
   */
  private List<String> writeDictionary(Dictionary dictionary,
                                       DictionaryService dictionaryService,
                                       DictionaryColumnUniqueIdentifier dictIdentifier,
                                       String storePath,
                                       Boolean isDictExists) throws IOException {
    List<String> distinctValues = new ArrayList<>();
    try {
      dictionaryWriter = dictionaryService
              .getDictionaryWriter(dictIdentifier.getCarbonTableIdentifier(),
                      dictIdentifier.getColumnIdentifier(), storePath);
      if (!isDictExists) {
        dictionaryWriter.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
      }
      // write value to dictionary file
      if (reverseIncrementalCache.size() >= 1) {
        if (isDictExists) {
          for (int i = 2; i < reverseIncrementalCache.size() + 2; i++) {
            String value = reverseIncrementalCache.get(i);
            String parsedValue = DataTypeUtil
                    .normalizeColumnValueForItsDataType(value, dimension);
            if (null != parsedValue && dictionary.getSurrogateKey(parsedValue) ==
                    CarbonCommonConstants.INVALID_SURROGATE_KEY) {
              dictionaryWriter.write(parsedValue);
              distinctValues.add(parsedValue);
            }
          }
        } else {
          for (int i = 2; i < reverseIncrementalCache.size() + 2; i++) {
            String value = reverseIncrementalCache.get(i);
            String parsedValue = DataTypeUtil
                    .normalizeColumnValueForItsDataType(value, dimension);
            if (null != parsedValue) {
              dictionaryWriter.write(parsedValue);
              distinctValues.add(parsedValue);
            }
          }
        }
      }
    } catch (IOException ex) {
      throw ex;
    } finally {
      if (null != dictionaryWriter) {
        dictionaryWriter.close();
      }
    }

    return distinctValues;
  }

  /**
   * write dictionary sort index to file
   *
   * @param distinctValues
   * @param dictionary
   * @param dictionaryService
   * @param tableIdentifier
   * @param columnIdentifier
   * @param storePath
   * @throws IOException
   */
  private void writeSortIndex(List<String> distinctValues,
                              Dictionary dictionary,
                              DictionaryService dictionaryService,
                              CarbonTableIdentifier tableIdentifier,
                              ColumnIdentifier columnIdentifier,
                              String storePath) throws IOException{
    CarbonDictionarySortIndexWriter carbonDictionarySortIndexWriter = null;
    try {
      if (distinctValues.size() > 0) {
        CarbonDictionarySortInfoPreparator preparator = new CarbonDictionarySortInfoPreparator();
        CarbonDictionarySortInfo dictionarySortInfo =
                preparator.getDictionarySortInfo(distinctValues, dictionary,
                        dimension.getDataType());
        carbonDictionarySortIndexWriter =
                dictionaryService.getDictionarySortIndexWriter(tableIdentifier, columnIdentifier,
                        storePath);
        carbonDictionarySortIndexWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
        carbonDictionarySortIndexWriter
                .writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
      }
    } catch (CarbonUtilException e) {
      e.printStackTrace();
    } finally {
      if (null != carbonDictionarySortIndexWriter) {
        carbonDictionarySortIndexWriter.close();
      }
    }
  }

  /**
   * update dictionary metadata
   */
  private void updateMetaData() throws IOException{
    if (null != dictionaryWriter) {
      dictionaryWriter.commit();
    }
  }

  /**
   * check dictionary file exists
   *
   * @param dictIdentifier
   * @param carbonTablePath
   * @return
   */
  private Boolean isDictionaryExists(DictionaryColumnUniqueIdentifier dictIdentifier,
                                     CarbonTablePath carbonTablePath) {
    String dictionaryFilePath =
            carbonTablePath.getDictionaryFilePath(dictIdentifier
                    .getColumnIdentifier().getColumnId());
    String dictionaryMetadataFilePath =
            carbonTablePath.getDictionaryMetaFilePath(dictIdentifier
                    .getColumnIdentifier().getColumnId());
    return CarbonUtil.isFileExists(dictionaryFilePath) && CarbonUtil
            .isFileExists(dictionaryMetadataFilePath);
  }
}
