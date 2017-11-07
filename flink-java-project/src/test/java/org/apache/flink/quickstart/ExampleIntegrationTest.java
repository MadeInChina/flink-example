package org.apache.flink.quickstart;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by pnowojski on 8/1/17.
 */
public class ExampleIntegrationTest extends StreamingMultipleProgramsTestBase {

    @Test
    public void testSum() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        CollectSink.values.clear();

        env.fromElements(1L, 21L, 22L)
                .map(new MultiplyByTwo())
                .addSink(new CollectSink());
        env.execute();

        assertEquals(Lists.newArrayList(2L, 42L, 44L), CollectSink.values);
    }

    private static class CollectSink implements SinkFunction<Long> {
        public static final List<Long> values = new ArrayList<>();

        @Override
        public void invoke(Long value) throws Exception {
            values.add(value);
        }
    }

    private class MultiplyByTwo implements MapFunction<Long, Long> {
        @Override
        public Long map(Long value) throws Exception {
            return value * 2;
        }
    }
}