package com.mapr.load;

import java.io.IOException;

public class BaseFiler {
  private TopTailAnalyzer[] analyzer = new TopTailAnalyzer[2];

  public BaseFiler() {
    for (int i = 0; i < analyzer.length; i++) {
      analyzer[i] = new TopTailAnalyzer();
    }
  }

  public void read(double t, int blockSize) throws IOException {
    throw new UnsupportedOperationException("default no can do");  //To change body of created methods use File | Settings | File Templates.
  }

  public void write(double t, int blockSize) throws IOException {
    throw new UnsupportedOperationException("default no can do");  //To change body of created methods use File | Settings | File Templates.
  }

  public void segmentEnd(double t) {
    // ignore
  }

  public void segmentStart(double t) {
    // ignore
  }

  public double currentTime() {
    throw new UnsupportedOperationException("default no can do");  //To change body of created methods use File | Settings | File Templates.
  }

  public void sleep(double delay) throws InterruptedException {
    throw new UnsupportedOperationException("default no can do");  //To change body of created methods use File | Settings | File Templates.
  }

  enum Op {READ, WRITE}

  public final void recordLatency(Op kind, double latency) {
    analyzer[kind.ordinal()].add(latency);
  }

  public final double quantiles(Op kind, int nines) {
    return analyzer[kind.ordinal()].quantile(nines);
  }

  public final long latencySamples(Op kind) {
    return analyzer[kind.ordinal()].size();
  }
}
