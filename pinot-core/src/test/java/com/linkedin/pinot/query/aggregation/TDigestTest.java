/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.aggregation;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.quantile.*;
import com.linkedin.pinot.core.query.aggregation.function.quantile.tdigest.TDigest;
import com.linkedin.pinot.util.TestUtils;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

import static org.testng.Assert.assertEquals;

/**
 *
 * Tests for TDigest quantile estimation, the routine is similar to {@link SimpleAggregationFunctionsTest}
 *
 *
 */
public class TDigestTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TDigestTest.class);
    private static final int compressionFactor = 100;
    private static final HashMap<Byte, TDigestFunction> functionMap = new HashMap<Byte, TDigestFunction>();
    private static final HashMap<Byte, QuantileAccurateFunction> accurateFunctionMap = new HashMap<Byte, QuantileAccurateFunction>();

    public static int[] _docIdsArray;
    public static IntArray _docIds;
    public static int _sizeOfDocIdArray = 5000;
    public static String _columnName = "met";
    public static AggregationInfo _paramsInfo;

    /**
     * This does not mean too much sense here, but we fix it to a small number.
     */
    private static final int DUPLICATION_PER_ITEM = 10;

    static {
        functionMap.put((byte) 50, new Quantile50());
        functionMap.put((byte) 90, new Quantile90());
        functionMap.put((byte) 95, new Quantile95());

        accurateFunctionMap.put((byte) 50, new QuantileAccurateFunction((byte) 50));
        accurateFunctionMap.put((byte) 90, new QuantileAccurateFunction((byte) 90));
        accurateFunctionMap.put((byte) 95, new QuantileAccurateFunction((byte) 95));
    }

    @BeforeClass
    public static void setup() {
        _docIdsArray = new int[_sizeOfDocIdArray];
        for (int i = 0; i < _sizeOfDocIdArray; ++i) {
            _docIdsArray[i] = i;
        }
        _docIds = new DefaultIntArray(_docIdsArray);
        Map<String, String> params = new HashMap<String, String>();
        params.put("column", _columnName);
        _paramsInfo = new AggregationInfo();
        _paramsInfo.setAggregationType("");
        _paramsInfo.setAggregationParams(params);
    }

    public static class RandomNumberArray {
        private static Random _rnd = new Random(System.currentTimeMillis());

        private final Integer[] arr;
        private final HashSet<Integer> set = new HashSet<Integer>();

        /**
         * Data ranges between [0, size)
         * @param size
         * @param duplicationPerItem
         */
        public RandomNumberArray(int size, int duplicationPerItem) {
            List<Integer> lst = new ArrayList<Integer>();
            for (int i = 0; i < size/duplicationPerItem; i++) {
                Integer item = _rnd.nextInt(size);
                for (int j = 0; j < duplicationPerItem; j++) {
                    lst.add(item); // add duplicates
                }
            }
            // add remaining items
            int st = lst.size();
            for (int i = st; i < size; i++) {
                Integer item = _rnd.nextInt(size);
                lst.add(item);
            }
            // add to set
            set.addAll(lst);
            // shuffle
            Collections.shuffle(lst);
            // toIntArray
            arr = lst.toArray(new Integer[0]);
            if (arr.length != size) {
                throw new RuntimeException("should not happen");
            }
        }

        public int size() {
            return arr.length;
        }

        public void offerAllNumberTo(TDigest tDigest) {
            offerNumberInRangeTo(tDigest, 0, arr.length);
        }

        public void offerAllNumberTo(DoubleArrayList list) {
            offerNumberInRangeTo(list, 0, arr.length);
        }

        public void offerNumberInRangeTo(TDigest tDigest, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                tDigest.offer(arr[i]);
            }
        }

        public void offerNumberInRangeTo(DoubleArrayList list, int start, int end) {
            end = Math.min(end, arr.length);
            for (int i = start; i < end; i++) {
                list.add(arr[i]);
            }
        }
    }

    @Test
    public void testCombineReduce() {
        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + " (combine)]");
            AggregationFunction aggregationFunction = functionMap.get(quantile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(quantile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            // Test combine
            int _sizeOfCombineList = 1000;
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (int i = 1; i <= _sizeOfCombineList; ++i) {
                List<Serializable> aggregationResults = getTDigestResultValues(i);
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                double estimate = ((TDigest) (combinedResult.get(0))).getQuantile(quantile);

                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(i);
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                double actual = (Double) QuantileUtil.getValueOnQuantile((DoubleArrayList) combinedResult2.get(0), quantile);

                TestUtils.assertApproximation(estimate, actual, 0.05);
                sb1.append((int)estimate + ", ");
                sb2.append(i + ", ");
            }
            assertEquals(sb1.toString(), sb2.toString());

            // Test reduce
            LOGGER.info("[Test Quantile " + quantile + " (reduce)]");
            for (int i = 1; i <= _sizeOfCombineList; ++i) {
              List<Serializable> combinedResults = getTDigestResultValues(i);
              List<Serializable> combinedResults2 = getDoubleArrayListResultValues(i);
              double estimate = (Double) aggregationFunction.reduce(combinedResults);
              double actual = (Double) aggregationAccurateFunction.reduce(combinedResults2);
              TestUtils.assertApproximation(estimate, actual, 0.05);
            }
        }
    }

    @Test
    public void testLargeCombineList() {
        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + "]");
            AggregationFunction aggregationFunction = functionMap.get(quantile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(quantile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            // Test combine
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            StringBuilder sb3 = new StringBuilder();

            // Make combine list number fixed to 10, each list has large number of elements
            int maxSize = 100000; // 10000000
            for (int i = 1; i <= maxSize; i += maxSize / 17) {
                if (i == 1) continue;
                RandomNumberArray arr = new RandomNumberArray(i * 10, 1);
                long t1 = System.nanoTime();
                List<Serializable> aggregationResults = getTDigestResultValues(arr, 10, i);
                long t2 = System.nanoTime();
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                long t3 = System.nanoTime();
                double estimate = ((TDigest) (combinedResult.get(0))).getQuantile(quantile);


                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(arr, 10, i);
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                double actual = (Double) QuantileUtil.getValueOnQuantile((DoubleArrayList) combinedResult2.get(0), quantile);

                TestUtils.assertApproximation(estimate, actual, 0.05);
                println(i + ", " + "" + (t2 - t1) + "" + ", " + (t3 - t2) + ", " + getErrorString(actual, estimate));
                // sb1.append(estimate + ", ");
                // sb2.append(precise + ", ");
            }
            // println("Error: " + sb3.toString());
            // assertEquals(sb1.toString(), sb2.toString());  // assert actual equals (nearly impossible!)
        }
    }

    @Test
    public void testRandomAggregationCombine() {
        final int numOfItemsPerList = 100;
        final int numOfListCombined = 1000;

        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + "]");
            AggregationFunction aggregationFunction = functionMap.get(quantile);
            AggregationFunction aggregationAccurateFunction = accurateFunctionMap.get(quantile);
            aggregationFunction.init(_paramsInfo);
            aggregationAccurateFunction.init(_paramsInfo);

            println("#list_combined, TDigest_time(nano), DoubleArrayList_time(nano), time_ratio, estimate, precise, error");

            // Test combine
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (int i = 1; i <= numOfListCombined; i += numOfListCombined / 17) {
                if (i == 1) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i * numOfItemsPerList, DUPLICATION_PER_ITEM);

                List<Serializable> aggregationResults = getTDigestResultValues(arr, i, numOfItemsPerList);
                long t1 = System.nanoTime();
                List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
                double estimate = ((TDigest) (combinedResult.get(0))).getQuantile(quantile);
                long t2 = System.nanoTime();

                List<Serializable> aggregationResults2 = getDoubleArrayListResultValues(arr, i, numOfItemsPerList);
                long t3 = System.nanoTime();
                List<Serializable> combinedResult2 = aggregationAccurateFunction.combine(aggregationResults2, CombineLevel.SEGMENT);
                double actual = (Double) QuantileUtil.getValueOnQuantile((DoubleArrayList) combinedResult2.get(0), quantile);
                long t4 = System.nanoTime();

                TestUtils.assertApproximation(estimate, actual, 0.05);
                println(i + ", " + (t2 - t1) + ", " + (t4 - t3) + ", " + (t2 - t1 + 0.0) / (t4 - t3 + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
            }
        }
    }

    @Test
    public void testInsertionTime() {
        int numOfItems = 1000000;

        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + "]");
            println("#items_inserted, TDigest_time(nano), DoubleArrayList_time(nano), time_ratio, estimate, precise, error");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                TDigest digest = new TDigest(compressionFactor);
                DoubleArrayList list = new DoubleArrayList();
                long t1 = System.nanoTime();
                arr.offerAllNumberTo(digest);
                double estimate = digest.getQuantile(quantile);
                long t2 = System.nanoTime();
                arr.offerAllNumberTo(list);
                double actual = QuantileUtil.getValueOnQuantile(list, quantile);
                long t3 = System.nanoTime();

                println(i + ", " + "" + (t2 - t1) + ", " + (t3 - t2) + ", " + (t2 - t1 + 0.0) / (t3 - t2 + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
            }
        }

        assertEquals(true, true);
    }

    @Test
    public void testMemoryConsumption() {
        int numOfItems = 1000000;

        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + "]");
            println("#items_inserted, TDigest_ser_size, DoubleArrayList_ser_size, ser_size_ratio, estimate, precise, error");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                TDigest digest = new TDigest(compressionFactor);
                DoubleArrayList list = new DoubleArrayList();
                arr.offerAllNumberTo(digest);
                arr.offerAllNumberTo(list);
                int digestSize = getSerializedSize(digest);
                int listSize = getSerializedSize(list);

                double estimate = digest.getQuantile(quantile);
                double actual = QuantileUtil.getValueOnQuantile(list, quantile);

                println(i + ", " + digestSize + ", " + listSize + ", " + (digestSize + 0.0) / (listSize + 0.0) + ", "
                        + estimate + ", " + actual + ", " + getErrorString(actual, estimate));
            }
        }

        assertEquals(true, true);
    }

    @Test
    public void testSerialization() {
        int numOfItems = 10000;

        for (Byte quantile: functionMap.keySet()) {
            LOGGER.info("[Test Quantile " + quantile + "]");
            for (int i = 0; i < numOfItems; i += numOfItems / 17) {
                if (i == 0) {
                    continue;
                }
                RandomNumberArray arr = new RandomNumberArray(i, DUPLICATION_PER_ITEM);
                TDigest digest = new TDigest(compressionFactor);
                DoubleArrayList list = new DoubleArrayList();
                arr.offerAllNumberTo(digest);
                arr.offerAllNumberTo(list);
                // write and read
                byte[] bytes = serialize(digest);
                TDigest digest2 = deserialize(bytes);

                double estimate = digest.getQuantile(quantile);
                double estimate2 = digest2.getQuantile(quantile);
                double actual = QuantileUtil.getValueOnQuantile(list, quantile);

                println("[Before Serialization Estimate]: " + estimate);
                println("[After Serialization Estimate]: " + estimate2);
            }
        }

        assertEquals(true, true);
    }

    // ------------ helper functions ------------

    private void println(String s) {
        System.out.println(s);
    }

    private String getErrorString(double precise, double estimate) {
        return Math.abs(precise - estimate + 0.0) /precise*100 + "%";
    }

    private int getSerializedSize(Serializable ser) {
        return serialize(ser).length;
    }

    private byte[] serialize(Serializable ser) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(ser);
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private TDigest deserialize(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bais);
            TDigest digest = (TDigest) ois.readObject();
            ois.close();
            return digest;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    // function set 1
    private static List<Serializable> getTDigestResultValues(int numOfListCombined) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            TDigest digest = new TDigest(compressionFactor);
            digest.offer(i);
            resultList.add(digest);
        }
        return resultList;
    }

    private static List<Serializable> getDoubleArrayListResultValues(int numOfListCombined) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            DoubleArrayList list = new DoubleArrayList();
            list.add(i);
            resultList.add(list);
        }
        return resultList;
    }

    // function set 2
    private static List<Serializable> getTDigestResultValues(RandomNumberArray arr, int numOfListCombined, int numOfItemsPerList) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numOfListCombined; ++i) {
            TDigest digest = new TDigest(compressionFactor);
            arr.offerNumberInRangeTo(digest, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            resultList.add(digest);
        }
        return resultList;
    }

    private static List<Serializable> getDoubleArrayListResultValues(RandomNumberArray arr, int numberOfListCombined, int numOfItemsPerList) {
        List<Serializable> resultList = new ArrayList<Serializable>();
        for (int i = 0; i < numberOfListCombined; ++i) {
            DoubleArrayList list = new DoubleArrayList();
            arr.offerNumberInRangeTo(list, i*numOfItemsPerList, (i+1)*numOfItemsPerList);
            resultList.add(list);
        }
        return resultList;
    }

    // others
    private TDigestFunction getQuantileAggregationFunction(byte quantile) {
        TDigestFunction ret = functionMap.get(quantile);
        if (ret != null) {
            return ret;
        }
        throw new RuntimeException("Quantile: " + quantile + " not supported!");
    }
}
