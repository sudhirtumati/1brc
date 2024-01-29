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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_sudhirtumati {

    private static final String FILE = "./measurements.txt";
    private static final int bufferSize = 8192;
    private static final byte SEMICOLON = (byte) ';';
    private static final byte NEW_LINE = (byte) '\n';
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final Semaphore PERMITS = new Semaphore(THREAD_COUNT);
    private static final MeasurementAggregator globalAggregator = new MeasurementAggregator();
    private static final Semaphore AGGREGATOR_PERMITS = new Semaphore(1);
    //private static final Map<FcPosKey, FcPosValue> FC_POS_MAP = new HashMap<>();
    /*private static final FileOutputStream fos;

    static {
        try {
            fos = new FileOutputStream("output.txt");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }*/

    public static void main(String[] args) throws IOException, InterruptedException {
        CalculateAverage_sudhirtumati instance = new CalculateAverage_sudhirtumati();
        //try(fos) {
            instance.chunkProcess();
        //}
    }

    static record FcPosKey(int index, String tId) implements Comparable<FcPosKey> {
        @Override
        public int compareTo(FcPosKey other) {
            return Objects.compare(this, other, Comparator.comparing(FcPosKey::index).thenComparing(FcPosKey::tId));
        }
    }

    static record FcPosValue(long start, long end) {
    }

    private void chunkProcess() throws InterruptedException {
        //Collection<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            PERMITS.acquire();
            Thread t = new ChunkProcessingThread(i);
            t.setName(STR."T\{i}");
            t.start();
        }
        while (PERMITS.availablePermits() != THREAD_COUNT) {
            Thread.sleep(100);
        }
        System.out.println(globalAggregator.getResult());
        //TreeMap<FcPosKey, FcPosValue> tm = new TreeMap<>(FC_POS_MAP);
        //System.out.println(tm);
    }

    static class ChunkProcessingThread extends Thread {

        private int index;
        private final MeasurementAggregator aggregator;

        ChunkProcessingThread(int index) {
            this.index = index;
            aggregator = new MeasurementAggregator();
        }

        @Override
        public void run() {
            try (FileInputStream is = new FileInputStream(FILE);
                 FileChannel fc = is.getChannel()) {
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                fc.position(index == 0 ? 0 : (((long) index * bufferSize) - 50));
                int lc = 1;
                while (fc.read(buffer) != -1) {
                    //System.out.println(fc.position());
                    buffer.flip();
                    printBuffer("BEFORE ADJUSTING", buffer);
                    if (fc.position() != bufferSize) {
                        seekStartPos(buffer);
                    }
                    //printBuffer(" AFTER ADJUSTING", buffer);
                    processBuffer(buffer);
                    //System.out.println(STR."\{Thread.currentThread().getName()}::\{index}::\{fcPos}::\{newFcPos}");
                    //FC_POS_MAP.put(new FcPosKey(index, Thread.currentThread().getName()), new FcPosValue(fc.position() - buffer.limit(), fc.position()));
                    index += THREAD_COUNT;
                    fc.position(((long) index * bufferSize) - (lc * 50L));
                    buffer.position(0);
                    lc++;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                AGGREGATOR_PERMITS.acquire();
                globalAggregator.process(aggregator);
                AGGREGATOR_PERMITS.release();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            PERMITS.release();
        }

        private void processBuffer(ByteBuffer buffer) throws IOException {
            int mStartMark = buffer.position();
            int tStartMark = -1;
            int count = buffer.position();
            do {
                byte b = buffer.get(count);
                if (b == SEMICOLON) {
                    tStartMark = count;
                } else if (b == NEW_LINE) {
                    byte[] locArr = new byte[tStartMark - mStartMark];
                    byte[] tempArr = new byte[count - tStartMark];
                    buffer.get(mStartMark, locArr);
                    buffer.get(mStartMark + locArr.length + 1, tempArr);
                    aggregator.process(locArr, tempArr);
                    mStartMark = count + 1;
                }
                count++;
            } while (count < buffer.limit());
        }

        private void printBuffer(String prefix, ByteBuffer buffer) {
            byte[] arr = new byte[buffer.limit() - buffer.position()];
            buffer.get(buffer.position(), arr);
            String str = new String(arr);
            str = str.replace('\n', ':');
            System.out.println(STR."\{prefix}:: \{Thread.currentThread().getName()}:: \{str}");
        }

        private void seekStartPos(ByteBuffer buffer) {
            for (int i = 49; i >= 0; i--) {
                if (buffer.get(i) == NEW_LINE) {
                    buffer.position(i + 1);
                    break;
                }
            }
        }
    }

    static final class MeasurementAggregator {
        private static final long MAX_VALUE_DIVIDE_10 = Long.MAX_VALUE / 10;
        private final Map<String, Measurement> store = new HashMap<>();

        public void process(MeasurementAggregator other) {
            other.store.forEach((k, v) -> {
                Measurement m = store.get(k);
                if (m == null) {
                    m = new Measurement();
                    store.put(k, m);
                }
                m.process(v);
            });
        }

        public void process(byte[] location, byte[] temperature) throws IOException {
            String loc = new String(location);
            Measurement measurement = store.get(loc);
            if (measurement == null) {
                measurement = new Measurement();
                store.put(loc, measurement);
            }
            double tempD = parseDouble(temperature);
            measurement.process(tempD);
            //System.out.print(STR."\{loc};\{tempD}::");
            //fos.write(STR."\{loc};\{tempD}\n".getBytes());
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

                } else if (ch == '.') {
                    decimalPlaces = 0;
                }
                if (index == bytes.length - 1) {
                    break;
                } else {
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

        public void process(Measurement other) {
            if (other.min < min) {
                this.min = other.min;
            }
            if (other.max > max) {
                this.max = other.max;
            }
            this.sum += other.sum;
            this.count += other.count;
        }

        public String toString() {
            ResultRow result = new ResultRow(min, sum, count, max);
            return result.toString();
        }
    }

    private record ResultRow(double min, double sum, double count, double max) {

        public String toString() {
            return STR."\{round(min)}/\{round(sum)}/\{round(count)}/\{round((Math.round(sum * 10.0) / 10.0) / count)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
