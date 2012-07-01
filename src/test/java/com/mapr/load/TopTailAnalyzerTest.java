package com.mapr.load;

import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class TopTailAnalyzerTest {
  @Test
  public void testQuantiles() {
    TopTailAnalyzer foo = new TopTailAnalyzer();
    Random gen = new Random();
    // quantiles of a uniform distribution are easy to compute
    for (int i = 0; i < 10000000; i++) {
      foo.add(gen.nextDouble());
    }

    assertEquals(0.99, foo.quantile(2), 0.01);
    assertEquals(0.999, foo.quantile(3), 0.002);
    assertEquals(0.9999, foo.quantile(4), 0.0001);
    assertEquals(0.99999, foo.quantile(5), 0.00001);
    assertEquals(0.999999, foo.quantile(6), 0.000001);
  }
}
