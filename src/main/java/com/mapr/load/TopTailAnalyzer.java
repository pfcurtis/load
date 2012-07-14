package com.mapr.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * Analyzes samples to determine the characteristics of the top tail of the distribution.
 */
public class TopTailAnalyzer {
  private final Random rand = new Random();

  // each of these keeps the top 1000 samples from all, or a sample of the data.
  // this allows us to compute high percentiles reasonably accurately with only
  // logarithmic storage.  The number of samples we need to keep is roughly
  // ceiling(log_10(n)) * 1000

  private final List<TreeSet<Double>> data = Lists.newArrayList();
  private double meanLatency;
  private double totalBytes;
  private double totalBlocks;
  private long samples;
  private double t0;

  public TopTailAnalyzer() {
    reset(0);
  }

  public void add(double latency, double bytes) {
    samples++;
    meanLatency += (latency - meanLatency) / samples;
    totalBytes += bytes;
    totalBlocks += 1;

    // add to each of the samples with progressively lower probability.
    // we use a loop based on index so we can add new elements to data in the loop.
    for (int i = 0; i < data.size(); i++) {
      // add to this sample and trim to top 1000
      final TreeSet<Double> x = data.get(i);
      if (x.size() < 1000 || latency > x.first()) {
        x.add(latency + rand.nextDouble() * 1e-12);
        if (x.size() > 1000) {
          x.remove(x.first());
        }
      }

      // with probability 0.1, add to the next sample
      final double u = rand.nextDouble();
      if (u <= 0.1) {
        // oh... that sample may not be there yet
        if (i == data.size() - 1) {
          addSampleTier();
        }
      } else {
        // if we didn't make the cut, we leave
        break;
      }
    }
  }

  /**
   * Resets all stats to zero.
   * @param t  Current time
   */
  public void reset(double t) {
    t0 = t;
    samples = 0;
    meanLatency = 0;
    data.clear();
    addSampleTier();
  }

  /**
   * Returns the mean latency.
   */
  public double meanLatency() {
    return meanLatency;
  }

  public double meanBytesPerSecond(double t) {
    return totalBytes / (t - t0);
  }

  public double meanBlocksPerSecond(double t) {
    return totalBlocks / (t - t0);
  }

  public double totalBytes() {
    return totalBytes;
  }

  /**
   * Analyzes the data seen so far and returns the (1-10^-nines) quantile.
   *
   * @param nines The number of 9's in the quantile desired.  That is 3 for the 99.9th %-ile.
   * @return The desired quantile.
   */
  public double quantile(int nines) {
    return quantile(1 - Math.pow(0.1, nines));
  }

  /**
   * Analyzes the data seen so far and returns the requested quantile.  If the (1-p) quantile is
   * requested, then the returned result should be between the (1-3p/2) and the (1-p/2) quantiles
   * with high probability and will often be between the (1-11p/10) and (1-9p/10) quantiles.
   *
   * @param q the desired quantile.
   * @return The desired quantile
   */
  public double quantile(double q) {
    Preconditions.checkArgument(q >= 0.99, "This summarizer only works on high quantiles");
    Preconditions.checkState(samples > 100, "Should have lots of samples before asking for a high quantile");

    // find the first usable set
    Iterator<TreeSet<Double>> i = data.iterator();
    double n = (1 - q) * samples;
    TreeSet<Double> x = i.next();
    while (n >= x.size()) {
      n /= 10;
      x = i.next();
    }
    // then estimate the quantile from that set
    return Iterables.get(x.descendingSet(), (int) n);
  }

  public long size() {
    return samples;
  }

  private void addSampleTier() {
    final TreeSet<Double> set = Sets.newTreeSet();
    data.add(set);
  }

  public double totalBlocks() {
    return totalBlocks;
  }
}
