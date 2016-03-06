package org.flinkspector.datastream.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.flinkspector.datastream.StreamTestBase;
import org.junit.Test;
import org.apache.flink.contrib.streaming.DataStreamUtils;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;
/**
 * Created by kknauf on 06.03.16.
 */
public class ProcessingTimeTest extends StreamTestBase {

    public static final double MAX_ERROR = 5.0;

    @Override
    protected TimeCharacteristic getTimeCharacteristic() {
        return TimeCharacteristic.ProcessingTime;
    }

    @Test
    public void testProcessingTime() {
        DataStream<String> testStream = createProcessingTimedTestStreamWith("record0").emit("record1", 50)
                                                                                            .emit("record2", 100)
                                                                                            .emit("record3", 150)
                                                                                            .emit("record4", 200)
                                                                                            .close();

        DataStream<Long> betweenElementTimes = testStream.map(new MapFunction<String, Long>() {

            long last = 0;

            @Override
            public Long map(String s) throws Exception {
                long now =  System.currentTimeMillis();
                long duration = now - last;
                last = now;
                return duration;
            }
        });

        Iterator<Long> iterator = DataStreamUtils.collect(betweenElementTimes);

        iterator.next(); // First element is just the current timestamnp

        while (iterator.hasNext()) {
            assertThat(Double.valueOf(iterator.next()), closeTo(50.0, MAX_ERROR));
        }



    }
}
