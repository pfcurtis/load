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
* Simple file access that does appending writes and random reads.
*/
class SimpleFiler extends Generator.RuntimeSystem {
  private OutputStream os;
  private Random rand = new Random();

  private FileChannel raf;

  private int writes;
  private long bytesWritten;
  private double segmentStart;

  private double t0 = this.currentTime();

  SimpleFiler() throws IOException {
    File outputFile = File.createTempFile("foo-", ".goo");
    outputFile.deleteOnExit();
    os = new FileOutputStream(outputFile);
    raf = new FileInputStream(outputFile).getChannel();
  }

  public void setReadFile(File f) throws IOException {
    raf = new FileInputStream(f).getChannel();
  }

  @Override
  public double currentTime() {
    return System.nanoTime() / 1e9;
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


  @Override
  public void read(double t, int blockSize) throws IOException {
    long position = (long) rand.nextInt((int) (raf.size() / blockSize)) * blockSize;
    raf.read(ByteBuffer.allocate(blockSize), position);
  }

  byte[] buf;

  @Override
  public void write(double t, int blockSize) throws IOException {
    // WATCHOUT: assumes single writer thread!
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
