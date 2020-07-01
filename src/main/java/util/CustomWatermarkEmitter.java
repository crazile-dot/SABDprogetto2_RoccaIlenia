package util;

import Query1.Failure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CustomWatermarkEmitter implements AssignerWithPeriodicWatermarks<Failure> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Failure element, long previousElementTimestamp) {
        long timestamp = element.getOccurredOn().getMillis();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}
