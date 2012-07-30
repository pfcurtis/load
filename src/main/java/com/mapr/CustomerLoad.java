package com.mapr;

/**
 * Simulates actual customer load.  This uses either of two traces that are
 * resources in the program and runs a load that consists of a mixture of a
 *
 * 1) database log which is sequentially written and then truncated and rewritten.
 *
 * 2) database file that is written with uneven random access
 *
 * 3) database reads that are uniform random
 *
 * 4) log writes that are sequential
 *
 * 5) log reads that read recently written blocks from (4)
 *
 * 6) every half hour, a substantial read occurs of about 6000 blocks over a
 * period of 5 seconds or so.
 *
 * Parameters are the proportion of writes that make up the total.  The reads
 * are determined by this because the reads in (5) are determined by the volume
 * of writes in (4).
 */

import com.mapr.load.*;
import java.io.File;
import java.io.IOException;

public class CustomerLoad {

   private static int numberOfThreads = 2;

   public static void main(String[] args) {
      for (int i = 0; i < numberOfThreads; i++) {
         Thread t = new GeneratorThread(args);
         t.start();
      }
   }
}
