package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import kafka.message.MessageAndMetadata;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.flume.source.avro.AvroFlumeEvent;

import java.util.Date;

public class Message2LogAppDHEvent extends Message2DHEvent {

    /**
     * 1、将消息头和消息体分别解析
     * 2、消息体转换成DHEvent
     * 3、从消息头中提取出timestamp，并插入到DHEvent中
     * @param message
     * @return
     * @throws Exception
     */
    @Override
    public DHEvent call(MessageAndMetadata<String,byte[]> message) throws Exception {
        DHEvent event = super.call(message);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message.message(), null);
        DatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);
        AvroFlumeEvent result = reader.read(null, decoder);

        String curTimeMs = result.getHeaders().get(new Utf8("timestamp")).toString();
        java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String curTime = formatter.format(new Date(Long.parseLong(curTimeMs)));
        String curHour = curTime.substring("yyyy-MM-dd ".length(), "yyyy-MM-dd ".length() + "HH".length());
        event.getTags().put("currentHour", curHour);
        return event;
    }

}
