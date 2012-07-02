package com.mapr.load;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

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

    final SimpleFiler actor = new SimpleFiler();
    actor.setReadFile(readFile);
    g.generate(actor);
  }

}
