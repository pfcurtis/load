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
      foo.add(gen.nextDouble(), gen.nextDouble()*100);
    }

    double p = 0.01;
    for (int i = 2; i < 6; i++) {
      final double q = foo.quantile(i);
      final double relativeError = (1 - q) / p - 1;
      System.out.printf("%d\t%.3f\n", i, relativeError);
      assertEquals(String.format("quantile(%f) was 1-%f, relative error", 1 - p, 1 - q),
        0, relativeError, 0.4);
      p /= 10;
    }
  }
}
