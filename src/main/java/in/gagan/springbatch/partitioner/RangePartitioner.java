package in.gagan.springbatch.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class RangePartitioner implements Partitioner {

    private final int max;

    public RangePartitioner(int max) {
        this.max = max;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long min = 1;
        long targetSize = (max - min) / gridSize + 1;
        System.out.println("targetSize : " + targetSize);

        Map<String, ExecutionContext> result = new HashMap<>();

        long number = 0;
        long start = min;
        long end = start + targetSize - 1;

        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number, value);

            if (end >= max) {
                end = max;
            }
            value.putLong("minValue", start);
            value.putLong("maxValue", end);
            start += targetSize;
            end += targetSize;
            number++;
        }
        System.out.println("partition result:" + result);
        return result;
    }
}
