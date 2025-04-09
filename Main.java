import java.io.*;
import java.util.*;
import java.util.concurrent.*;

class ProcessThread extends Thread {
    int pid, burstTime;

    public ProcessThread(int pid, int burstTime) {
        this.pid = pid;
        this.burstTime = burstTime;
    }

    public void run() {
        System.out.println("Process " + pid + " started.");
        try {
            Thread.sleep(burstTime * 1000);
        } catch (InterruptedException e) {}
        System.out.println("Process " + pid + " finished.");
    }
}

// Producer-Consumer Problem
class BoundedBuffer {
    Queue<Integer> buffer = new LinkedList<>();
    int capacity = 5;

    Semaphore mutex = new Semaphore(1);
    Semaphore items = new Semaphore(0);
    Semaphore space = new Semaphore(capacity);

    public void produce(int item) throws InterruptedException {
        System.out.println("[Producer] Waiting to add item " + item);
        space.acquire();
        mutex.acquire();
        buffer.add(item);
        System.out.println("[Producer] Produced item: " + item);
        mutex.release();
        items.release();
    }

    public int consume() throws InterruptedException {
        System.out.println("[Consumer] Waiting to consume item...");
        items.acquire();
        mutex.acquire();
        int item = buffer.remove();
        System.out.println("[Consumer] Consumed item: " + item);
        mutex.release();
        space.release();
        return item;
    }
}

class Producer extends Thread {
    BoundedBuffer buffer;

    public Producer(BoundedBuffer buffer) {
        this.buffer = buffer;
    }

    public void run() {
        try {
            for (int i = 1; i <= 5; i++) {
                buffer.produce(i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {}
    }
}

class Consumer extends Thread {
    BoundedBuffer buffer;

    public Consumer(BoundedBuffer buffer) {
        this.buffer = buffer;
    }

    public void run() {
        try {
            for (int i = 1; i <= 5; i++) {
                buffer.consume();
                Thread.sleep(1500);
            }
        } catch (InterruptedException e) {}
    }
}

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting process threads...");

        // Read from processes.txt
        List<ProcessThread> threads = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("processes.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.strip().split(" ");
                int pid = Integer.parseInt(parts[0]);
                int burst = Integer.parseInt(parts[1]);
                threads.add(new ProcessThread(pid, burst));
            }
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        System.out.println("\nProcess threads completed.\n");

        // Producer-Consumer Section
        System.out.println("Starting Producer-Consumer simulation...\n");
        BoundedBuffer buffer = new BoundedBuffer();
        Producer producer = new Producer(buffer);
        Consumer consumer = new Consumer(buffer);

        producer.start();
        consumer.start();

        producer.join();
        consumer.join();

        System.out.println("\nProducer-Consumer simulation complete.");
    }
}
