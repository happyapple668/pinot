package com.linkedin.thirdeye.hadoop.topk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.MetricType;

public class TopKPhaseMapOutputValue {

  Number[] metricValues;
  List<MetricType> metricTypes;

  public TopKPhaseMapOutputValue(Number[] metricValues, List<MetricType> metricTypes) {
    this.metricValues = metricValues;
    this.metricTypes = metricTypes;
  }

  public Number[] getMetricValues() {
    return metricValues;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    // metric values
    dos.writeInt(metricValues.length);
    for (int i = 0; i < metricValues.length; i++) {
      Number number = metricValues[i];
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          dos.writeShort(number.intValue());
          break;
        case LONG:
          dos.writeLong(number.longValue());
          break;
        case INT:
          dos.writeInt(number.intValue());
          break;
        case FLOAT:
          dos.writeFloat(number.floatValue());
          break;
        case DOUBLE:
          dos.writeDouble(number.doubleValue());
          break;
      }
    }

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static TopKPhaseMapOutputValue fromBytes(byte[] buffer, List<MetricType> metricTypes) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;

    // metric values
    length = dis.readInt();
    Number[] metricValues = new Number[length];

    for (int i = 0 ; i < length; i++) {
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          metricValues[i] = dis.readShort();
          break;
        case LONG:
          metricValues[i] = dis.readLong();
          break;
        case INT:
          metricValues[i] = dis.readInt();
          break;
        case FLOAT:
          metricValues[i] = dis.readFloat();
          break;
        case DOUBLE:
          metricValues[i] = dis.readDouble();
          break;
      }
    }

    TopKPhaseMapOutputValue wrapper;
    wrapper = new TopKPhaseMapOutputValue(metricValues, metricTypes);
    return wrapper;
  }

}