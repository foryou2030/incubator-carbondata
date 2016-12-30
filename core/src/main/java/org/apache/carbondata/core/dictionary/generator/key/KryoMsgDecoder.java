package org.apache.carbondata.core.dictionary.generator.key;

import java.io.ByteArrayInputStream;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * 自定义Decoder
 * @author Ricky
 *
 */
public class KryoMsgDecoder extends FrameDecoder {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(KryoMsgDecoder.class.getName());

  public static final int HEAD_LENGTH = 4;

  private Kryo kryo = new Kryo();

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer in) throws Exception {

    if (in.readableBytes() < HEAD_LENGTH) {  //这个HEAD_LENGTH是我们用于表示头长度的字节数。  由于Encoder中我们传的是一个int类型的值，所以这里HEAD_LENGTH的值为4.
      return null;
    }
    in.markReaderIndex();                  //我们标记一下当前的readIndex的位置
    int dataLength = in.readInt();       // 读取传送过来的消息的长度。ByteBuf 的readInt()方法会让他的readIndex增加4
    if (dataLength < 0) { // 我们读到的消息体长度为0，这是不应该出现的情况，这里出现这情况，关闭连接。
      channel.close();
    }

    if (in.readableBytes() < dataLength) { //读到的消息体长度如果小于我们传送过来的消息长度，则resetReaderIndex. 这个配合markReaderIndex使用的。把readIndex重置到mark的地方
      in.resetReaderIndex();
      return null;
    }

    byte[] body = new byte[dataLength];  //传输正常
    in.readBytes(body);
    DictionaryKey out = (DictionaryKey) convertToObject(body);  //将byte数据转化为我们需要的对象
    return out;
  }

  private Object convertToObject(byte[] body) {

    Input input = null;
    ByteArrayInputStream bais = null;
    try {
      bais = new ByteArrayInputStream(body);
      input = new Input(bais);

      return kryo.readObject(input, DictionaryKey.class);
    } catch (KryoException e) {
      LOGGER.error("Error while covert " + body.toString() + " to DictionaryKey");
      e.printStackTrace();
    }finally{
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(bais);
    }

    return null;
  }

}
