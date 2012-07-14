package com.mapr.load;

import java.io.IOException;


public interface Filer {
  void read(double t, int blockSize) throws IOException;

  void write(double t, int blockSize) throws IOException;

  void segmentEnd(double t);

  void segmentStart(double t);

  double currentTime();

  void sleep(double delay) throws InterruptedException;

  void recordLatency(Op kind, double latency, double bytes);

  double quantiles(Op kind, int nines);

  long latencySamples(Op kind);

  void reset(double t);

  public enum Op {READ, WRITE}
}
