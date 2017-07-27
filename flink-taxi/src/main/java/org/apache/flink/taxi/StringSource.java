package org.apache.flink.taxi;

import java.io.IOException;

/**
 * Created by pnowojski on 6/19/17.
 */
public class StringSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<String> {

    private final long numberOfElements;
    private final long waitTime;
    private boolean cancel;

    public StringSource(long numberOfElements, long waitTime) {
        this.numberOfElements = numberOfElements;
        this.waitTime = waitTime;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (long i = 1; i < numberOfElements && !cancel; i++) {
            Thread.sleep((waitTime > 0) ? waitTime : 0);
            sourceContext.collect(String.format("%d %d", i, i + 1497790546981L));
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
