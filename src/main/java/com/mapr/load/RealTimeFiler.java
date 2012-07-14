package com.mapr.load;

/**
 * Adds real-time behavior.
 */
public class RealTimeFiler extends BaseFiler {
  private double speedup;

  public RealTimeFiler(double speedup) {
    this.speedup = speedup;
  }

  @Override
  public double currentTime() {
    return System.nanoTime() * speedup / 1e9;
  }

  @Override
  public void sleep(double delay) throws InterruptedException {
    Thread.sleep((long) (delay * 1e3 / speedup));
  }

  public static RealTimeFiler create(double speedup) {
    return new RealTimeFiler(speedup);
  }
}
