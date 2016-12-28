// automatically generated, do not modify

package org.apache.carbondata.core.dictionary.generator.key;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

public class FlatbDictKey extends Table {
  public static FlatbDictKey getRootAsFlatbDictKey(ByteBuffer _bb) { return getRootAsFlatbDictKey(_bb, new FlatbDictKey()); }
  public static FlatbDictKey getRootAsFlatbDictKey(ByteBuffer _bb, FlatbDictKey obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public FlatbDictKey __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String tableUniqueName() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer tableUniqueNameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String columnName() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer columnNameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public String data() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public String type() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer typeAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public String threadNo() { int o = __offset(12); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer threadNoAsByteBuffer() { return __vector_as_bytebuffer(12, 1); }

  public static int createFlatbDictKey(FlatBufferBuilder builder,
                                       int tableUniqueName,
                                       int columnName,
                                       int data,
                                       int type,
                                       int threadNo) {
    builder.startObject(5);
    FlatbDictKey.addThreadNo(builder, threadNo);
    FlatbDictKey.addType(builder, type);
    FlatbDictKey.addData(builder, data);
    FlatbDictKey.addColumnName(builder, columnName);
    FlatbDictKey.addTableUniqueName(builder, tableUniqueName);
    return FlatbDictKey.endFlatbDictKey(builder);
  }

  public static void startFlatbDictKey(FlatBufferBuilder builder) { builder.startObject(5); }
  public static void addTableUniqueName(FlatBufferBuilder builder, int tableUniqueNameOffset) { builder.addOffset(0, tableUniqueNameOffset, 0); }
  public static void addColumnName(FlatBufferBuilder builder, int columnNameOffset) { builder.addOffset(1, columnNameOffset, 0); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(2, dataOffset, 0); }
  public static void addType(FlatBufferBuilder builder, int typeOffset) { builder.addOffset(3, typeOffset, 0); }
  public static void addThreadNo(FlatBufferBuilder builder, int threadNoOffset) { builder.addOffset(4, threadNoOffset, 0); }
  public static int endFlatbDictKey(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

