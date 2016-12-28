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
package org.apache.carbondata.core.dictionary.server;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.carbondata.core.dictionary.generator.ServerDictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

import com.alibaba.fastjson.JSON;

import org.apache.carbondata.core.dictionary.generator.key.FlatbDictKey;
import org.jboss.netty.channel.*;

import java.nio.ByteBuffer;


/**
 * Handler for Dictionary server.
 */
public class DictionaryServerHandler extends SimpleChannelHandler {

  /**
   * dictionary generator
   */
  private ServerDictionaryGenerator generatorForServer = new ServerDictionaryGenerator();

  /**
   * channel connected
   *
   * @param ctx
   * @param e
   * @throws Exception
   */
  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    System.out.println("Connected " + ctx.getHandler());
  }

  /**
   * receive message and handle
   *
   * @param ctx
   * @param e
   * @throws Exception
   */
  @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {
//    String keyString = (String) e.getMessage();
//    DictionaryKey key = JSON.parseObject(keyString, DictionaryKey.class);
    ByteBuffer rbb = ((FlatBufferBuilder) e.getMessage()).dataBuffer();
    DictionaryKey key = new DictionaryKey();
    FlatbDictKey fkey = FlatbDictKey.getRootAsFlatbDictKey(rbb);
    key.setTableUniqueName(fkey.tableUniqueName());
    key.setColumnName(fkey.columnName());
    key.setData(Integer.parseInt(fkey.data()));
    key.setType(fkey.type());
    key.setThreadNo(fkey.threadNo());
    int outPut = processMessage(key);
    key.setData(outPut);
    // Send back the response
//    String backkeyString = JSON.toJSONString(key);
    FlatBufferBuilder fbb = new FlatBufferBuilder();
    int tableUniqueName = fbb.createString(key.getTableUniqueName());
    int columnName = fbb.createString(key.getColumnName());
    int data = fbb.createString(key.getData().toString());
    int type = fbb.createString(key.getType());
    int threadNo = fbb.createString(key.getThreadNo());
    int root = FlatbDictKey.createFlatbDictKey(fbb, tableUniqueName, columnName, data, type, threadNo);
    fbb.finish(root);
    ctx.getChannel().write(fbb);
    super.messageReceived(ctx, e);
  }

  /**
   * handle exceptions
   *
   * @param ctx
   * @param e
   */
  @Override public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    e.getCause().printStackTrace();
    Channel ch = e.getChannel();
    ch.close();
  }

  /**
   * process message by message type
   *
   * @param key
   * @return
   * @throws Exception
   */
  public Integer processMessage(DictionaryKey key) throws Exception {
    switch (key.getType()) {
      case "DICTIONARY_GENERATION":
        return generatorForServer.generateKey(key);
      case "TABLE_INITIALIZATION":
        generatorForServer.initializeGeneratorForTable(key);
        return 0;
      case "SIZE":
        return generatorForServer.size(key);
      case "WRITE_DICTIONARY":
        generatorForServer.writeDictionaryData();
        return 0;
      default:
        return -1;
    }
  }

}
