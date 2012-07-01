package com.mapr.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Analyzes samples to determine the characteristics of the top distribution tail.
 */
public class TopTailAnalyzer {
  private Random rand = new Random();

  // each of these keeps the top 1000 samples from all, 1% or 0.01% of the data.
  // this allows us to compute high percentiles reasonably accurately with even
  // as much as a billion samples.
  PriorityQueue<Double> p1 = new PriorityQueue<Double>();
  PriorityQueue<Double> p2 = new PriorityQueue<Double>();
  PriorityQueue<Double> p3 = new PriorityQueue<Double>();
  int samples;

  public void add(double delta) {
    double u = rand.nextDouble();
    samples++;
    stash(u, 1, p1, delta);
    stash(u, 0.01, p2, delta);
    stash(u, 1e-4, p3, delta);
  }

  /**
   * Analyzes the data seen so far and returns the (1-10^-nines) quantile.
   * @param nines    The number of 9's in the quantile desired.  That is 3 for the 99.9th %-ile.
   * @return  The desired quantile.
   */
  public double quantile(int nines) {
    Preconditions.checkArgument(nines > 1, "This summarizer doesn't make sense for smaller quantiles");
    Preconditions.checkState(samples > 10, "Should have lots of samples before asking for a high quantile");

    double p = Math.pow(0.1, nines);
    double n = p * samples;
    if (n > 1000) {
      n = p * samples * 0.01;
      if (n > 1000) {
        n = p * samples * 0.0001;
        if (n > 1000) {
          throw new UnsupportedOperationException(String.format("Can't get the 1-%f quantile from %d samples", p, samples));
        } else {
          return nthBest(n, p3);
        }
      } else {
        return nthBest(n, p2);
      }
    } else {
      return nthBest(n, p1);
    }
  }

  private double nthBest(double n, PriorityQueue<Double> queue) {
    List<Double> tmp = Lists.newArrayList(queue);
    Collections.sort(tmp, Ordering.natural().reverse());
    return tmp.get((int) n);
  }

  private void stash(double u, double p, PriorityQueue<Double> queue, double delta) {
    if (u <= p) {
      if (queue.size() < 1000 || delta > queue.peek()) {
        queue.add(delta);
        while (queue.size() > 1000) {
          queue.poll();
        }
      }
    }
  }

}
