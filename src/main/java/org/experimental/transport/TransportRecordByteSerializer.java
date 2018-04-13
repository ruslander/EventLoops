package org.experimental.transport;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

public class TransportRecordByteSerializer implements Serializer<TransportRecord>, Deserializer<TransportRecord> {
    private Gson recordGson = new Gson();

    @Override
    public TransportRecord deserialize(String s, byte[] bytes) {
        TransportRecord record = null;
        try(Reader rdr = new InputStreamReader(new ByteArrayInputStream(bytes))) {
            record = recordGson.fromJson(rdr, TransportRecord.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }

    @Override
    public byte[] serialize(String s, TransportRecord record) {
        byte[] retVal = null;
        try {
            retVal = recordGson.toJson(record).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }
}
