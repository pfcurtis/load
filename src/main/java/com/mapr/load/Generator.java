package com.mapr.load;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A generator generates read and write load according to a trace file.  During each period
 * specified by a trace file, reads and writes are generated at the specified rate with
 * Poisson distributed timing.
 *
 * All of the read, write and timing semantics are handled by an object passed into the
 * generator.  This makes it easy to inject semantics appropriate for testing.
 */
public class Generator {
  private Random rand = new Random();

  // total seconds in the trace
  private double totalLength = 0;
  private final List<LoadSegment> trace = Lists.newArrayList();

  private int blockSize;

  private double t0;

  /**
   * Generate events for the entire trace at rates specified in the trace.
   *
   * @param actor    Where to send the events.
   */
  public void generate(RuntimeSystem actor) throws InterruptedException, IOException {
    generate(0, totalLength, 1, actor);
  }

  /**
   * Generates read and write activity for a specified time period on a particular runtime.
   *
   *
   * @param offset   How far into the trace to go before generating events.
   * @param duration How many seconds of activity to generate.
   * @param scale    How much faster or slow should events be generated relative to what is in the
   *                 trace.
   * @param actor    Where to send the events.
   * @throws InterruptedException If a timer is aborted.
   */
  public void generate(double offset, double duration, double scale, RuntimeSystem actor) throws InterruptedException, IOException {
    t0 = actor.currentTime();
    double t = t0;
    for (LoadSegment segment : trace) {
      if (t + segment.getSegmentDuration() < offset) {
        // skip segments that don't matter
        t += segment.getSegmentDuration();
      } else if (t > t0 + offset + duration) {
        // stop if we have passed the end
        break;
      } else {
        final double end = Math.min(t + segment.getSegmentDuration(), offset + duration + t0) - offset;
        if (offset + t0 > t) {
          // first part of segment is out of bounds
          t = generateSegment(t, offset + t0, 0, 0, null);
        }
        t = generateSegment(t, end, scale * segment.getReadRate(), scale * segment.getWriteRate(), actor);
      }
    }
  }

  private double generateSegment(double t, double end, double readRate, double writeRate, RuntimeSystem actor) throws InterruptedException, IOException {
    // total event rate
    final double rate = readRate + writeRate;
    // what portion are reads?
    final double readP = readRate / rate;

    actor.segmentStart(t);
    t = Math.min(end, t + expSample(rate));
    while (t < end) {
      // only really sleep if there is a millisecond or more to sleep
      final double t1 = actor.currentTime();
      if (t - t1 > 1e-3) {
        actor.sleep(t - t1);
      }

      if (actor.currentTime() > t + 5) {
        throw new IOException("Generator fell more than 5 seconds behind ... aborting run");
      }

      // select type of transaction
      if (rand.nextDouble() < readP) {
        actor.read(t, blockSize);
      } else {
        actor.write(t, blockSize);
      }

      t = Math.min(end, t + expSample(rate));
    }
    if (t - actor.currentTime() > 1e-3) {
      actor.sleep(t - actor.currentTime());
    }
    actor.segmentEnd(t);
    return end;
  }

  /**
   * Reads a trace file from a file assuming a standard format that consists of:
   *
   * a header line with three tab delimited fields
   *
   * lots of data lines that have a date and two counts (writes and reads)
   *
   * The data line fields are tab delimited as well and the date format is MM/DD HH:mm:ss.
   * Data lines with insufficient fields are ignored.  Data lines with an invalid date time
   * cause an exception.
   *
   * @param f  The file to read
   * @return  A list of data segments.
   * @throws IOException If the file can't be read
   */
  public static List<LoadSegment> readTraceFile(File f) throws IOException {
    return Files.readLines(f, Charsets.UTF_8, new LineProcessor<List<LoadSegment>>() {
      public Splitter onTabs = Splitter.on("\t").trimResults().omitEmptyStrings();
      public DateFormat df = new SimpleDateFormat("M/d H:m:s");
      public List<LoadSegment> trace = Lists.newArrayList();
      double t0 = 0;
      double reads = 0;
      double writes = 0;
      boolean header = true;

      public boolean processLine(String line) throws IOException {
        Iterable<String> bits = onTabs.split(line);
        if (Iterables.size(bits) == 3) {
          Iterator<String> i = bits.iterator();
          final String dateString = i.next();
          try {
            double t = df.parse(dateString).getTime()/1e3;
            if (t0 != 0) {
              trace.add(new LoadSegment(reads, writes, t - t0));
            }
            t0 = t;
            writes = Double.parseDouble(i.next());
            reads = Double.parseDouble(i.next());
          } catch (ParseException e) {
            if (!header) {
              throw new IOException(String.format("Bad date <%s>\n", dateString));
            } else {
              header = false;
            }
          }
        }
        return true;
      }

      public List<LoadSegment> getResult() {
        return trace;
      }
    });
  }

  /**
   * Sample from an exponential distribution.  Accumulating these delays
   * gives us a Poisson process with the specified rate (in events per second)
   * @param rate  The rate at which events should happen.
   * @return  An exponentially distributed delay time.
   */
  private double expSample(double rate) {
    if (rate > 0) {
      return -Math.log(1 - rand.nextDouble()) / rate;
    } else {
      return Double.MAX_VALUE;
    }
  }

  /**
   * Adds a set of trace events to this generator.
   * @param trace   A list of load segments to add to the trace.
   */
  public void addTrace(List<LoadSegment> trace) {
    this.trace.addAll(trace);
    for (LoadSegment segment : trace) {
      totalLength += segment.getSegmentDuration();
    }
  }

  /**
   * Sets the block size in bytes for reading and writing.
   * @param blockSize  The size in bytes.
   */
  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public static class LoadSegment {
    private double readRate;
    private double writeRate;
    private double segmentDuration;

    public LoadSegment(double readRate, double writeRate, double segmentDuration) {
      this.readRate = readRate;
      this.writeRate = writeRate;
      this.segmentDuration = segmentDuration;
    }

    public double getSegmentDuration() {
      return segmentDuration;
    }

    public double getReadRate() {
      return readRate;
    }

    public double getWriteRate() {
      return writeRate;
    }
  }

  public static abstract class RuntimeSystem {
    private Random rand = new Random();

    public abstract void read(double t, int blockSize) throws IOException;

    public abstract void write(double t, int blockSize) throws IOException;

    public void segmentEnd(double t) {
      // ignore
    }

    public void segmentStart(double t) {
      // ignore
    }

    public abstract double currentTime();

    public abstract void sleep(double delay) throws InterruptedException;

    // each of these keeps the top 1000 samples from all, 1% or 0.01% of the data.
    // this allows us to compute high percentiles reasonably accurately
    PriorityQueue<Double> p1 = new PriorityQueue<Double>();
    PriorityQueue<Double> p2 = new PriorityQueue<Double>();
    PriorityQueue<Double> p3 = new PriorityQueue<Double>();
    int samples;
    public void recordLatency(double delta) {
      double u = rand.nextDouble();
      samples++;
      stash(u, 1, p1, delta);
      stash(u, 0.01, p2, delta);
      stash(u, 1e-4, p3, delta);
    }

    public double percentile(int nines) {
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
      List<Double> tmp = Lists.newArrayList();
      for (Double x : queue) {
        tmp.add(x);
      }
      return tmp.get((int) (tmp.size() - n));
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
}
