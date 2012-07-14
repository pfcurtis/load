package com.mapr.load;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.io.Files;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GeneratorTest {
  enum Kind {
    START, END, READ, WRITE
  }

  private static class Event {
    double t;
    Kind type;

    private Event(double t, Kind type) {
      this.t = t;
      this.type = type;
    }
  }

  @Test
  public void testSegments() throws InterruptedException, IOException {
    final List<Event> history = Lists.newArrayList();
    Generator g = new Generator();
    g.addTrace(Lists.newArrayList(
      new Generator.LoadSegment(0, 0, 1.1),
      new Generator.LoadSegment(0, 0, 2.3),
      new Generator.LoadSegment(0, 0, 0.2)
    ));

    g.generate(new Recorder(history, 1e-3));

    Iterator<Event> i = history.iterator();
    assertEquals(0, i.next().t - Recorder.EPOCH, 1e-9);
    assertEquals(1.1, i.next().t - Recorder.EPOCH, 1e-9);
    assertEquals(1.1, i.next().t - Recorder.EPOCH, 1e-9);
    assertEquals(3.4, i.next().t - Recorder.EPOCH, 1e-9);
    assertEquals(3.4, i.next().t - Recorder.EPOCH, 1e-9);
    assertEquals(3.6, i.next().t - Recorder.EPOCH, 1e-9);
    assertFalse(i.hasNext());
  }

  @Test
  public void testRates() throws InterruptedException, IOException {
    final List<Event> history = Lists.newArrayList();
    Generator g = new Generator();
    g.addTrace(Lists.newArrayList(
      new Generator.LoadSegment(1000, 0, 1.1),
      new Generator.LoadSegment(0, 1000, 2.3),
      new Generator.LoadSegment(100, 2000, 1.2)
    ));

    g.generate(new Recorder(history, 1e-3));

    List<Multiset<Kind>> counts = Lists.newArrayList();
    Multiset<Kind> current = HashMultiset.create();
    for (Event event : history) {
      switch (event.type) {
        case END:
          counts.add(current);
          current = HashMultiset.create();
          break;
        case START:
          // ignore
          break;
        case READ:
        case WRITE:
          current.add(event.type);
          break;
      }
    }
    assertEquals(0, current.size());

    assertEquals(3, counts.size());
    assertEquals(1100, counts.get(0).count(Kind.READ), 3 * Math.sqrt(1100));
    assertEquals(0, counts.get(0).count(Kind.WRITE));

    assertEquals(0, counts.get(1).count(Kind.READ));
    assertEquals(2300, counts.get(1).count(Kind.WRITE), 3 * Math.sqrt(2300));

    assertEquals(120, counts.get(2).count(Kind.READ), 3 * Math.sqrt(120));
    assertEquals(2400, counts.get(2).count(Kind.WRITE), 3 * Math.sqrt(2400));
  }

  @Test
  public void testTraceReader() throws IOException {
    File f = File.createTempFile("trace-", "tsv");
    f.deleteOnExit();
    Files.write("03p nfs\twrite\tread\n" +
      "06/28 09:00:00\t0.00\t20.00\n" +
      "06/28 09:00:05\t1.00\t21.00\n" +
      "06/28 09:00:07\t\t\n" +
      "06/28 09:00:08\t2.00\t22.00\n" +
      "06/28 09:00:09\t3.00\t23.00\n", f, Charsets.UTF_8);
    List<Generator.LoadSegment> x = Generator.readTraceFile(f);
    assertEquals(3, x.size());
    int i = 0;
    for (Generator.LoadSegment segment : x) {
      assertEquals(20 + i, segment.getReadRate(), 0);
      assertEquals(i, segment.getWriteRate(), 0);
      i++;
    }
    assertEquals(5, x.get(0).getSegmentDuration(), 0);
    assertEquals(3, x.get(1).getSegmentDuration(), 0);
    assertEquals(1, x.get(2).getSegmentDuration(), 0);
  }

  @Test
  public void testRealIo() throws IOException, InterruptedException {
    Assume.assumeNotNull(System.getProperty("slowTests"));
    Generator g = new Generator();
    List<Generator.LoadSegment> trace = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      trace.add(new Generator.LoadSegment(0, i * 500, 2));
    }
    g.addTrace(trace);

    g.setBlockSize(4096);

    g.generate(new Appender());
  }

  private class Appender extends BaseFiler {
    private OutputStream os;
    private Random rand = new Random();

    private int writes;
    private long bytesWritten;
    private double segmentStart;

    private double t0 = this.currentTime();

    private Appender() throws IOException {
      File outputFile = File.createTempFile("foo-", ".goo");
      outputFile.deleteOnExit();
      os = new FileOutputStream(outputFile);
    }

    @Override
    public double currentTime() {
      return System.nanoTime() / 1e9;
    }

    @Override
    public void read(double t, int blockSize) {
      throw new UnsupportedOperationException("Can't read in an appender");
    }

    @Override
    public void segmentEnd(double t) {
      System.out.printf("%.3f end (writes = %d, %.2f/s, %.1f MB/s\n", t - t0, writes, writes / (currentTime() - segmentStart), bytesWritten / (currentTime() - segmentStart) / 1e6);
      if (latencySamples(Op.WRITE) > 100) {
        System.out.printf("%10d %.3f %.3f %.3f %.3f\n", writes, quantiles(Op.WRITE, 2), quantiles(Op.WRITE, 3), quantiles(Op.WRITE, 4), quantiles(Op.WRITE, 5));
      }
      writes = 0;
      segmentStart = currentTime();
      bytesWritten = 0;
    }

    @Override
    public void segmentStart(double t) {
      System.out.printf("%.3f start %.3f slippage\n", t - t0, currentTime() - t);
    }

    @Override
    public void sleep(double delay) throws InterruptedException {
      final long sleepTime = (long) (delay * 1e3);
      Thread.sleep(sleepTime);
    }

    byte[] buf;

    @Override
    public void write(double t, int blockSize) throws IOException {
      if (buf == null || buf.length != blockSize) {
        buf = new byte[blockSize];
        rand.nextBytes(buf);
      }
      writes++;
      bytesWritten += blockSize;

      double t0 = currentTime();
      os.write(buf);
      os.flush();
      double t1 = currentTime();
      super.recordLatency(Op.WRITE, t1 - t0, blockSize);
    }
  }

  private class Recorder extends BaseFiler {
    private static final double EPOCH = 12345.34;

    private double t = EPOCH;
    private List<Event> history;
    private double quantum;

    public Recorder(List<Event> history, double quantum) {
      super();
      this.history = history;
      this.quantum = quantum;
    }


    @Override
    public void segmentStart(double t) {
      history.add(new Event(t, Kind.START));
    }

    @Override
    public void segmentEnd(double t) {
      history.add(new Event(t, Kind.END));
    }

    @Override
    public void read(double t, int blockSize) {
      history.add(new Event(t, Kind.READ));
    }

    @Override
    public void write(double t, int blockSize) {
      history.add(new Event(t, Kind.WRITE));
    }

    @Override
    public double currentTime() {
      return t;
    }

    @Override
    public void sleep(double delay) {
      t += 1e-3 * Math.ceil(delay / quantum);
    }
  }
}
