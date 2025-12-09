package org.itmo.kafka;

import java.util.concurrent.Callable;

public class ConsumerRunner implements Callable<Void> {

    private final Runnable consumerStart;
    private final String name;

    public ConsumerRunner(String name, Runnable consumerStart) {
        this.name = name;
        this.consumerStart = consumerStart;
    }

    @Override
    public Void call() {
        try {
            System.out.println("Starting consumer: " + name);
            consumerStart.run();
            System.out.println("Consumer finished: " + name);
        } catch (Exception e) {
            System.err.println("Consumer crashed: " + name);
            e.printStackTrace();
        }
        return null;
    }
}
