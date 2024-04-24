package jpoint2024;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private final static Logger logger = LoggerFactory.getLogger("main");

    public static void main(String[] args) {
        var stream = new Stream();
        stream.configure();
        stream.start();

        var scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(stream::logAssignedPartitions, 5, 3, TimeUnit.SECONDS);
    }
}
