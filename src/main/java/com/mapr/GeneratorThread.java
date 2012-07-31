package com.mapr;

import com.mapr.load.*;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class GeneratorThread extends Thread {
   private String[] args;

   public GeneratorThread(String[] a) {
      this.args = a;
   }

   public void run() {
      File file = null;

      Generator g = new Generator();
      g.setBlockSize(4096);

      try {
         String hostname = java.net.InetAddress.getLocalHost().getHostName();
         file = new File(hostname + "-" + this.getId());
         file.deleteOnExit();

         final Filer actor = RandomFiler.create(file, 1000000, 1, 1);
         for (String trace : args) {
            System.out.println("actor.currentTime(): " + actor.currentTime());
            actor.reset(actor.currentTime());
            System.out.println("Adding trace "+trace);
            g.addTrace(Generator.readTraceFile(new File(trace)));
            g.generate(0,250000,0.2,actor);
         }
      } catch (Exception e) {
         return;
      }

   }
}


