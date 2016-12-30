package org.apache.carbondata.core.dictionary.generator.key;

import java.io.ByteArrayOutputStream;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;


public class KryoMsgEncoder extends OneToOneEncoder {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(KryoMsgDecoder.class.getName());

  private Kryo kryo = new Kryo();

  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {

    byte[] body = convertToBytes((DictionaryKey) msg);  //将对象转换为byte
    int dataLength = body.length;  //读取消息的长度
    ChannelBuffer out = ChannelBuffers.dynamicBuffer();
    out.writeInt(dataLength);  //先将消息长度写入，也就是消息头
    out.writeBytes(body);  //消息体中包含我们要发送的数据
    return out;
  }

  private byte[] convertToBytes(DictionaryKey key) {
    ByteArrayOutputStream bos = null;
    Output output = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, key);
      output.flush();

      return bos.toByteArray();
    } catch (KryoException e) {
      LOGGER.error("Error while covert " + key.toString() + " to byte[]");
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(output);
      IOUtils.closeQuietly(bos);
    }
    return null;
  }

}