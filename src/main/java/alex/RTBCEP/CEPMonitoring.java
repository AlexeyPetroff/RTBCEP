package alex.RTBCEP;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;


import java.util.List;
import java.util.Map;

public class CEPMonitoring {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.enableCheckpointing(1000).
            setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> messageStream = env
                .addSource(new FlinkKafkaConsumer09<>(
                                    parameterTool.getRequired("topic"),
                                    new EventDeserializer(),
                                    parameterTool.getProperties()))
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        DataStream<Event> partitionedByAdId = messageStream.keyBy(
                new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.getAd_id();
                    }
        });

        DataStream<Event> partitionedByDevId = messageStream.keyBy(
                new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.getDev_id();
                    }
                });

        // Warning pattern: click macros may be set wrong
        Pattern<Event, ?> lowCTR = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getType().equals("impression");
            }
        }).times(1000).notFollowedBy("clicked").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getType().equals("click");
            }
        }).next("any");


        Pattern<Event, ?> changedCountry = Pattern.<Event>begin("first")
                .next("second")
                .within(Time.minutes(10));

        Pattern<Event, ?> fastClicks = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getType().equals("impression");
            }
        }).followedBy("click").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getType().equals("click");
            }
        }).within(Time.seconds(1)).times(3);


        // Create a pattern stream from lowCTR
        PatternStream<Event> patternStreamLowCTR = CEP.pattern(partitionedByAdId, lowCTR);

        PatternStream<Event> patternStreamFastClicks = CEP.pattern(partitionedByDevId, fastClicks);

        PatternStream<Event> patternStreamChangedCountry = CEP.pattern(partitionedByDevId, changedCountry);

        DataStream<LowCTR> alarms = patternStreamLowCTR.select(new PatternSelectFunction<Event, LowCTR>() {
            @Override
            public LowCTR select(Map<String, List<Event>> pattern) throws Exception {
                Event first = (Event) pattern.get("first").get(0);
                return new LowCTR(first.getAd_id());
            }
        });

        DataStream<ClickFraud> alarmsClicks = patternStreamFastClicks.select(new PatternSelectFunction<Event, ClickFraud>() {
            @Override
            public ClickFraud select(Map<String, List<Event>> pattern) throws Exception {
                Event first = (Event) pattern.get("first").get(0);
                return new ClickFraud(first.getDev_id());
            }
        });

        DataStream<ClickFraud> alarmsChangedCountry = patternStreamChangedCountry.select(new PatternSelectFunction<Event, ClickFraud>() {
            @Override
            public ClickFraud select(Map<String, List<Event>> pattern) throws Exception {
                Event first = (Event) pattern.get("first").get(0);
                Event second = (Event) pattern.get("second").get(0);
                if (first.getCountry() != second.getCountry()){
                    return new ClickFraud(first.getDev_id());
                }
                return null;
            }
        });

        alarms.map(v -> v.toString()).writeAsText(parameterTool.getRequired("out"), WriteMode.OVERWRITE);
        alarmsClicks.map(v -> v.toString()).writeAsText(parameterTool.getRequired("out"), WriteMode.OVERWRITE);
        alarmsChangedCountry.map(v -> v != null ? v.toString(): "").writeAsText(parameterTool.getRequired("out"), WriteMode.OVERWRITE);

        messageStream.map(v -> v.toString()).print();

        env.execute("Flink monitoring job");

    }
}
