/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function.quantile;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Accurate Quantile function, it is used for ground truth.
 * However, it should never be used in production since in reduce step,
 * it will sort a large list which incurs significant overhead.
 *
 */
public class QuantileAccurateFunction implements AggregationFunction<DoubleArrayList, Double> {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuantileAccurateFunction.class);
  public static final int DEFAULT_COMPRESSION_FACTOR = 100;

  private String _columnName;
  private byte _quantile; // 0-100

  public QuantileAccurateFunction(byte quantile) {
    _quantile = quantile;
  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _columnName = aggregationInfo.getAggregationParams().get("column");
  }

  private void offerValue(int docId, Block[] block, DoubleArrayList list) {
    Dictionary dictionaryReader = block[0].getMetadata().getDictionary();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    if (blockValIterator.skipTo(docId)) {
      int dictionaryIndex = blockValIterator.nextIntVal();
      if (dictionaryIndex != Dictionary.NULL_VALUE_INDEX) {
        list.add(((Integer) dictionaryReader.get(dictionaryIndex)));
      } else {
        // ignore this
        LOGGER.info("ignore NULL_VALUE_INDEX");
      }
    }
  }

  @Override
  public DoubleArrayList aggregate(Block docIdSetBlock, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (type != DataType.INT) {
      throw new RuntimeException("Currently support Int type, get: " + type);
    }

    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    DoubleArrayList ret = new DoubleArrayList();
    int docId = 0;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      offerValue(docId, block, ret);
    }

    return ret;
  }

  @Override
  public DoubleArrayList aggregate(DoubleArrayList mergedResult, int docId, Block[] block) {
    DataType type = block[0].getMetadata().getDataType();
    if (type != DataType.INT) {
      throw new RuntimeException("Only support Int type, get: " + type);
    }

    if (mergedResult == null) {
      mergedResult = new DoubleArrayList();
    }
    offerValue(docId, block, mergedResult);
    return mergedResult;
  }

  @Override
  public List<DoubleArrayList> combine(List<DoubleArrayList> aggregationResultList, CombineLevel combineLevel) {
    if ((aggregationResultList == null) || aggregationResultList.isEmpty()) {
      return null;
    }

    DoubleArrayList list = new DoubleArrayList();
    for (DoubleArrayList aggregationResult : aggregationResultList) {
      list.addAll(aggregationResult);
    }
    aggregationResultList.clear();
    aggregationResultList.add(list);
    return aggregationResultList;
  }

  @Override
  public DoubleArrayList combineTwoValues(DoubleArrayList aggregationResult0, DoubleArrayList aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }

    aggregationResult0.addAll(aggregationResult1);
    return aggregationResult0;
  }

  /**
   * naive and slow implementation
   * @param combinedResultList
   * @return
   */
  @Override
  public Double reduce(List<DoubleArrayList> combinedResultList) {
    if ((combinedResultList == null) || combinedResultList.isEmpty()) {
      return 0.0;
    }

    DoubleArrayList list = new DoubleArrayList();
    for (DoubleArrayList aggregationResult : combinedResultList) {
      list.addAll(aggregationResult);
    }

    return QuantileUtil.getValueOnQuantile(list, _quantile);
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      return new JSONObject().put("value", finalAggregationResult.toString());
    } catch (JSONException e) {
      LOGGER.error("Caught exception while rendering aggregation result", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.OBJECT;
  }

  @Override
  public String getFunctionName() {
    return "quantileAccurate" + _quantile + "_" + _columnName;
  }

  @Override
  public Serializable getDefaultValue() {
    return new DoubleArrayList();
  }
}
