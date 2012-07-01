package com.mapr.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 6/29/12 Time: 10:57 PM To change this template
 * use File | Settings | File Templates.
 */
public class Load {
  public static void main(String[] args) throws IOException, InterruptedException {
    Generator g = new Generator();
    for (String trace : args) {
      g.addTrace(Generator.readTraceFile(new File(trace)));
    }

    g.setBlockSize(4096);

    File readFile = new File("read-file.goo");
    OutputStream os = new FileOutputStream(readFile);
    Random rand = new Random();
    byte[] buf = new byte[4096];
    for (int i = 0; i < 1e9 / 4096; i++) {
      rand.nextBytes(buf);
      os.write(buf);
    }
    os.close();

    final Filer actor = new Filer();
    actor.setReadFile(readFile);
    g.generate(actor);
  }

  private static class Filer extends Generator.RuntimeSystem {
    private OutputStream os;
    private Random rand = new Random();

    private FileChannel raf;

    private int writes;
    private long bytesWritten;
    private double segmentStart;

    private double t0 = this.currentTime();

    private Filer() throws IOException {
      File outputFile = File.createTempFile("foo-", ".goo");
      outputFile.deleteOnExit();
      os = new FileOutputStream(outputFile);
    }

    public void setReadFile(File f) throws IOException {
      raf = new FileInputStream(f).getChannel();
    }

    @Override
    public double currentTime() {
      return System.nanoTime() / 1e9;
    }

    @Override
    public void read(double t, int blockSize) throws IOException {
      long position = (long) rand.nextInt((int) (raf.size() / blockSize)) * blockSize;
      raf.read(ByteBuffer.allocate(blockSize), position);
    }

    @Override
    public void segmentEnd(double t) {
      System.out.printf("%.3f end (writes = %d, %.2f/s, %.1f MB/s\n", t - t0, writes, writes / (t - segmentStart), bytesWritten / (t - segmentStart) / 1e6);
    }

    @Override
    public void segmentStart(double t) {
      System.out.printf("%.3f start\n", t - t0);
      writes = 0;
      segmentStart = t;
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
      os.write(buf);
      os.flush();
    }
  }

}
