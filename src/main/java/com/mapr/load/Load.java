package com.mapr.load;

import java.io.File;
import java.io.IOException;

public class Load {
  public static void main(String[] args) throws IOException, InterruptedException {
    Generator g = new Generator();
    g.setBlockSize(4096);

    File file = new File("file.goo");
    file.deleteOnExit();

    final Filer actor = RandomFiler.create(file, 1000000, 1, 1);

    for (String trace : args) {
      actor.reset(actor.currentTime());
      g.addTrace(Generator.readTraceFile(new File(trace)));
      g.generate(actor);
    }
  }
}
