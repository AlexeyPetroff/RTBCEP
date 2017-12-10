package alex.RTBCEP;

import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

 public class EventDeserializer implements KeyedDeserializationSchema<Event> {
    private Gson gson;
    @Override
    public Event deserialize(byte[] messageKey,
                                   byte[] message,
                                   String topic,
                                   int partition,
                                   long offset) throws IOException {
        if (gson == null) {
            gson = new Gson();
        }
        Event m = gson.fromJson(new String(message), Event.class);
        return m;

    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return getForClass(Event.class);
    }
}