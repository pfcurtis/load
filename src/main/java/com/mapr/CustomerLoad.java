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
 * period of 5 seconds or so.  This represents about 5MB/sec read rate.
 *
 * Parameters are the proportion of writes that make up the total.  The reads
 * are determined by this because the reads in (5) are determined by the volume
 * of writes in (4).
 */

import com.google.common.collect.Lists;
import com.mapr.load.Filer;

import java.util.List;
import java.util.concurrent.*;

public class CustomerLoad {

   private static int numberOfThreads = 1;

   public static void main(String[] args) throws InterruptedException, ExecutionException {
       List<Callable<Filer>> tasks = Lists.newArrayList();
       for (int i = 0; i < numberOfThreads; i++) {
           tasks.add(new GeneratorThread(args));
       }
       ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
       List<Future<Filer>> results = pool.invokeAll(tasks);
       pool.shutdown();

       // can iterate through results here to aggregate stats.
       for (Future<Filer> result : results) {
           System.out.printf("result = %.3f\n", result.get().quantiles(Filer.Op.READ, 10));
       }
   }
}
