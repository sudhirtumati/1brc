/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_sudhirtumati {

    private static final String FILE = "./measurements.txt";

    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(100);

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 100L, TimeUnit.MILLISECONDS, queue);

    public static void main(String[] args) throws IOException, InterruptedException {
        CalculateAverage_sudhirtumati instance = new CalculateAverage_sudhirtumati();
        instance.process();
        System.out.println(MeasurementAggregator.getInstance().getResult());
    }

    private void process() throws IOException, InterruptedException {
        int bufferSize = 8192 * 128;
        MeasurementAggregator aggregator = MeasurementAggregator.getInstance();
        executor.prestartAllCoreThreads();
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r");
                FileChannel fc = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            ByteBuffer leftoverBuf = ByteBuffer.allocate(0);
            while (fc.read(buffer) > 0) {
                buffer.flip();
                ByteBuffer resultBuf = ByteBuffer.allocate(leftoverBuf.capacity() + buffer.limit()).put(leftoverBuf).put(buffer);
                int endIndex = findIndexOfValidEnd(resultBuf);
                ByteBuffer toProcessBuf = resultBuf.slice(0, endIndex);
                queue.offer(new BufferChunkProcessor(deepCopy(toProcessBuf), aggregator), 10, TimeUnit.SECONDS);
                resultBuf.position(0);
                endIndex++;
                leftoverBuf = resultBuf.slice(endIndex, resultBuf.limit() - endIndex);
                buffer.clear();
            }
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private ByteBuffer deepCopy(ByteBuffer original) {
        int pos = original.position();
        int lim = original.limit();
        try {
            original.position(0).limit(original.capacity());
            ByteBuffer copy = doDeepCopy(original);
            copy.position(pos).limit(lim);
            return copy;
        }
        finally {
            original.position(pos).limit(lim);
        }
    }

    private ByteBuffer doDeepCopy(ByteBuffer original) {
        int pos = original.position();
        try {
            ByteBuffer copy = ByteBuffer.allocate(original.remaining());
            copy.put(original);
            copy.order(original.order());
            return copy.position(0);
        }
        finally {
            original.position(pos);
        }
    }

    int findIndexOfValidEnd(ByteBuffer buffer) {
        int endIndex = -1;
        int pos = buffer.limit() - 1;
        while (endIndex == -1 && pos > -1) {
            if ((char) buffer.get(pos) == '\n') {
                endIndex = pos;
            }
            pos--;
        }
        return endIndex;
    }

    static final class BufferChunkProcessor implements Runnable {

        private static final byte SEMICOLON = (byte) ';';
        private static final byte NEW_LINE = (byte) '\n';
        private final ByteBuffer buffer;
        private final MeasurementAggregator measurementAggregator;

        BufferChunkProcessor(ByteBuffer buffer, MeasurementAggregator measurementAggregator) {
            this.buffer = buffer;
            this.measurementAggregator = measurementAggregator;
        }

        @Override
        public void run() {
            int mStartMark = 0;
            int tStartMark = -1;
            buffer.position(0);
            int count = 0;
            do {
                byte b = buffer.get(count);
                if (b == SEMICOLON) {
                    tStartMark = count;
                }
                else if (b == NEW_LINE || count == buffer.limit() - 1) {
                    byte[] locArr = new byte[tStartMark - mStartMark];
                    byte[] tempArr = new byte[count - tStartMark];
                    buffer.get(mStartMark, locArr);
                    buffer.get(mStartMark + locArr.length + 1, tempArr);
                    measurementAggregator.process(locArr, tempArr);
                    mStartMark = count + 1;
                }
                count++;
            } while (count < buffer.limit());
        }
    }

    static final class MeasurementAggregator {
        private static final MeasurementAggregator instance = new MeasurementAggregator();
        private static final long MAX_VALUE_DIVIDE_10 = Long.MAX_VALUE / 10;
        private final Map<String, Measurement> store = new ConcurrentHashMap<>();

        private MeasurementAggregator() {
        }

        public static MeasurementAggregator getInstance() {
            return instance;
        }

        public void process(byte[] location, byte[] temperature) {
            String loc = new String(location);
            Measurement measurement = store.get(loc);
            if (measurement == null) {
                measurement = new Measurement();
                store.put(loc, measurement);
            }
            double tempD = parseDouble(temperature);
            measurement.process(tempD);
        }

        public double parseDouble(byte[] bytes) {
            long value = 0;
            int exp = 0;
            boolean negative = false;
            int decimalPlaces = Integer.MIN_VALUE;
            int index = 0;
            int ch = bytes[index];
            if (ch == '-') {
                negative = true;
                ch = bytes[++index];
            }
            while (index < bytes.length) {
                if (ch >= '0' && ch <= '9') {
                    while (value >= MAX_VALUE_DIVIDE_10) {
                        value >>>= 1;
                        exp++;
                    }
                    value = value * 10 + (ch - '0');
                    decimalPlaces++;

                }
                else if (ch == '.') {
                    decimalPlaces = 0;
                }
                if (index == bytes.length - 1) {
                    break;
                }
                else {
                    ch = bytes[++index];
                }
            }
            return asDouble(value, exp, negative, decimalPlaces);
        }

        private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
            if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
                if (value < Long.MAX_VALUE / (1L << 32)) {
                    exp -= 32;
                    value <<= 32;
                }
                if (value < Long.MAX_VALUE / (1L << 16)) {
                    exp -= 16;
                    value <<= 16;
                }
                if (value < Long.MAX_VALUE / (1L << 8)) {
                    exp -= 8;
                    value <<= 8;
                }
                if (value < Long.MAX_VALUE / (1L << 4)) {
                    exp -= 4;
                    value <<= 4;
                }
                if (value < Long.MAX_VALUE / (1L << 2)) {
                    exp -= 2;
                    value <<= 2;
                }
                if (value < Long.MAX_VALUE / (1L << 1)) {
                    exp -= 1;
                    value <<= 1;
                }
            }
            for (; decimalPlaces > 0; decimalPlaces--) {
                exp--;
                long mod = value % 5;
                value /= 5;
                int modDiv = 1;
                if (value < Long.MAX_VALUE / (1L << 4)) {
                    exp -= 4;
                    value <<= 4;
                    modDiv <<= 4;
                }
                if (value < Long.MAX_VALUE / (1L << 2)) {
                    exp -= 2;
                    value <<= 2;
                    modDiv <<= 2;
                }
                if (value < Long.MAX_VALUE / (1L << 1)) {
                    exp -= 1;
                    value <<= 1;
                    modDiv <<= 1;
                }
                if (decimalPlaces > 1)
                    value += modDiv * mod / 5;
                else
                    value += (modDiv * mod + 4) / 5;
            }
            final double d = Math.scalb((double) value, exp);
            return negative ? -d : d;
        }

        public String getResult() {
            Map<String, Measurement> sortedMap = new TreeMap<>(store);
            return sortedMap.toString();
        }
    }

    static final class Measurement {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public void process(double value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

        public String toString() {
            ResultRow result = new ResultRow(min, sum, count, max);
            return result.toString();
        }
    }

    private record ResultRow(double min, double sum, double count, double max) {

        public String toString() {
            return round(min) + "/" + round((Math.round(sum * 10.0) / 10.0) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
