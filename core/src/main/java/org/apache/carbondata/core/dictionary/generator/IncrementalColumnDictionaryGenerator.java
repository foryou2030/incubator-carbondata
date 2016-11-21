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
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.service.PathService;
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

  private int maxDictionary;

  private CarbonDimension dimension;

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
      }
      return dict;
    }
  }

  @Override public void writeDictionaryData(DictionaryKey key) throws IOException {
    // write data to file system
    CarbonTableIdentifier tableIdentifier = key.getTableIdentifier();
    ColumnIdentifier columnIdentifier = key.getColumnIdentifier();
    String storePath = key.getStorePath();

    DictionaryService dictionaryService = CarbonCommonFactory.getDictionaryService();
    CarbonDictionaryWriter dictionaryWriter =
            dictionaryService.getDictionaryWriter(tableIdentifier, columnIdentifier, storePath);

    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService.getCarbonTablePath(storePath, tableIdentifier);

    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
            cacheProvider.createCache(CacheType.REVERSE_DICTIONARY, carbonTablePath.getTableStatusFilePath());
    DictionaryColumnUniqueIdentifier identifier =
            new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier, columnIdentifier.getDataType());
    Dictionary dictionary = null;
    try {
      dictionary = cache.get(identifier);
    } catch (CarbonUtilException e) {
      System.out.println("Didn't find dictionary from cache! ");
      dictionary = null;
    }

    List<String> distinctValues = new ArrayList<>();
    // write dictionary
    try {
      //TODO: isFileExists replace dictionaryIsNull
      if (dictionary == null) {
        dictionaryWriter.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
      }
      if (incrementalCache.size() > 1) {
        // TODO: map need sort first
        for (String value : incrementalCache.keySet()) {
          String parsedValue = DataTypeUtil
                  .normalizeColumnValueForItsDataType(value, dimension);
          if (null != parsedValue) {
            if (dictionary == null) {
              dictionaryWriter.write(parsedValue);
              distinctValues.add(parsedValue);
            } else {
              if (dictionary.getSurrogateKey(parsedValue) ==
                      CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                dictionaryWriter.write(parsedValue);
                distinctValues.add(parsedValue);
              }
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
    // write sort index
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
}
