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
package org.apache.carbondata.processing.newflow.dictionary;

import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.apache.carbondata.core.dictionary.generator.key.MESSAGETYPE;

/**
 * Dictionary implementation along with dictionary server client to get new dictionary values
 */
public class DictionaryServerClientDictionary implements BiDictionary<Integer, Object> {

  private Dictionary dictionary;

  private DictionaryClient client;

  private Map<Object, Integer> localCache;

  private DictionaryKey dictionaryKey;

  private Object lock = new Object();

  public DictionaryServerClientDictionary(Dictionary dictionary, DictionaryClient client,
      DictionaryKey key, Map<Object, Integer> localCache) {
    this.dictionary = dictionary;
    this.client = client;
    this.dictionaryKey = key;
    this.localCache = localCache;
  }

  @Override public Integer getOrGenerateKey(Object value) throws DictionaryGenerationException {
    Integer key = getKey(value);
    if (key == null) {
      synchronized (lock) {
        dictionaryKey.setData(value);
        dictionaryKey.setThreadNo(Thread.currentThread().getId() + "");
        DictionaryKey dictionaryValue = client.getDictionary(dictionaryKey);
        key = (Integer) dictionaryValue.getData();
        localCache.put(value, key);
      }
      int size = dictionary.getDictionaryChunks().getSize();
      return key + (size == 0 ? size : size - 1);
    }
    return key;
  }

  @Override public Integer getKey(Object value) {
    Integer key = dictionary.getSurrogateKey(value.toString());
    if (key == CarbonCommonConstants.INVALID_SURROGATE_KEY ) {
      key = localCache.get(value);
      if (key != null) {
        int size = dictionary.getDictionaryChunks().getSize();
        return key + (size == 0 ? size : size - 1);
      }
    }
    return key;
  }

  @Override public Object getValue(Integer key) {
    throw new UnsupportedOperationException("Not supported here");
  }

  @Override public int size() {
    dictionaryKey.setMessage(MESSAGETYPE.SIZE);
    int size = (int) client.getDictionary(dictionaryKey).getData()
            + dictionary.getDictionaryChunks().getSize();
    return size;
  }
}
