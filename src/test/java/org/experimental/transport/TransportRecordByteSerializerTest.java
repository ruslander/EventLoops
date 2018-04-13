package org.experimental.transport;

import com.google.gson.Gson;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TransportRecordByteSerializerTest {
    TransportRecordByteSerializer serializer = new TransportRecordByteSerializer();
    private Gson recordGson = new Gson();

    @Test
    public void encode(){
        TransportRecord record = new TransportRecord(UUID.randomUUID(), new HashMap<>());
        Assert.assertEquals(serializer.serialize("", record), recordGson.toJson(record).getBytes());
    }

    @Test
    public void decode(){
        UUID uuid = UUID.randomUUID();
        HashMap<String, String> c1 = new HashMap<>();
        c1.put("a", "b");
        TransportRecord record = new TransportRecord(uuid, c1);
        byte[] recordBytes = recordGson.toJson(record).getBytes();

        TransportRecord record1 = serializer.deserialize("", recordBytes);
        Map<String, String> c2 = record1.getContent();

        Assert.assertEquals(record1.getUuid(), record.getUuid());
        Assert.assertEquals(c2.get("a"), "b");
    }

    @Test
    public void pack(){
        TransportRecord record = new TransportRecord(UUID.randomUUID(), new HashMap<>());
        System.out.println(recordGson.toJson(record));
    }
}
