package com.mapr.load;

import com.mapr.generate.ChineseRestaurant;
import com.mapr.generate.Sampler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * Adds power law distributed writes to a filer chain.
 */
public class RandomFiler extends RealTimeFiler {
  private double sequential = 0;
  private double writes = 0;

  private Sampler<Integer> sampler;
  private ByteBuffer buf;
  private FileChannel raf;
  private Random rand = new Random();

  private double t0 = -1;

  public static RandomFiler create(File f) throws IOException {
    return new RandomFiler(f, new ChineseRestaurant(100000, 0), 1);
  }

  public static RandomFiler create(File f, double alpha, double discount, double timeRate) throws FileNotFoundException {
    return new RandomFiler(f, new ChineseRestaurant(alpha, discount), timeRate);
  }

  public RandomFiler(File f, Sampler<Integer> sampler, double timeRate) throws FileNotFoundException {
    super(timeRate);
    System.out.printf("%s\n", f);
    raf = new RandomAccessFile(f, "rw").getChannel();
    this.sampler = sampler;
  }

  int messageSparseCount = 0;

  @Override
  public void read(double t, int blockSize) throws IOException {
    if (t0 < 0) {
      t0 = this.currentTime();
    }
    // WATCHOUT: assumes single reader thread!
    if (buf == null || buf.capacity() != blockSize) {
      buf = ByteBuffer.allocate(blockSize);
    }
    long block = rand.nextInt((int) (raf.size() / blockSize));
    double t0 = currentTime();
    buf.position(0);
    buf.limit(blockSize);
    raf.read(buf, block * blockSize);
    buf.flip();

    if (buf.getLong() != block) {
      throw new IOException("Block has incorrect content");
    }
    double t1 = currentTime();
    recordLatency(Op.READ, t1 - t0, blockSize);
  }


  public void write(double t, int blockSize) throws IOException {
    if (t0 < 0) {
      t0 = this.currentTime();
    }

    // WATCHOUT: assumes single writer thread!
    if (buf == null || buf.capacity() != blockSize) {
      buf = ByteBuffer.allocate(blockSize);
      rand.nextBytes(buf.array());
    }

    long block = sampler.sample();

    writes++;
    if (block * blockSize == raf.size()) {
      sequential += (1 - sequential) / writes;
    } else {
      sequential += -sequential / writes;
    }

    double t0 = currentTime();
    buf.position(0);
    buf.putLong(block);
    buf.limit(blockSize);
    buf.position(0);
    raf.write(buf, block * blockSize);
    double t1 = currentTime();
    recordLatency(Op.WRITE, t1 - t0, blockSize);
  }

  @Override
  public void segmentEnd(double t) {
    System.out.printf("sequential %% = %.3f\n", sequential);
    sequential = 0;
    writes = 0;

    super.segmentEnd(t);    //To change body of overridden methods use File | Settings | File Templates.
  }
}
