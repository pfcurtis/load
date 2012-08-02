package com.mapr;

import com.mapr.load.Filer;
import com.mapr.load.Generator;
import com.mapr.load.RandomFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class GeneratorThread implements Callable<Filer> {
    private static AtomicInteger idCounter = new AtomicInteger();
    private final int id = idCounter.addAndGet(1);

    private Logger log = LoggerFactory.getLogger(GeneratorThread.class);
    private String[] args;

    public GeneratorThread(String[] a) {
        this.args = a;
    }

    @Override
    public Filer call() throws IOException, InterruptedException {
        Generator g = new Generator();
        g.setBlockSize(4096);

        String hostname = java.net.InetAddress.getLocalHost().getHostName();
        File file = new File(hostname + "-" + id);
        file.deleteOnExit();

        final Filer actor = RandomFiler.create(file, 1000000, 0, 1);
        for (String trace : args) {
            actor.reset(actor.currentTime());
            log.debug("Adding trace {}", trace);
            g.addTrace(Generator.readTraceFile(new File(trace)));
            g.generate(0, Double.MAX_VALUE, 1, actor);
        }
        return actor;
    }
}



