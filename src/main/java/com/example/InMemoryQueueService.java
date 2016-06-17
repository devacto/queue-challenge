package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class InMemoryQueueService implements QueueService {

  private Map<String, ArrayBlockingQueue<Message>> queueMap = new HashMap<>();

  public InMemoryQueueService(Map<String, ArrayBlockingQueue<Message>> queues) {
    this.queueMap = queues;
  }

  public void push(String queueUrl, String message) throws IOException, InterruptedException {
    ArrayBlockingQueue<Message> queue = queueMap.get(queueUrl);

    if (queue == null) {
      throw new IOException(String.format("unknown queue %s", queueUrl));
    } else {
      queue.put(new Message(message));
    }

  }

  public Message pull(String queueUrl) throws IOException {
    ArrayBlockingQueue<Message> queue = queueMap.get(queueUrl);

    if (queue == null) {
      throw new IOException(String.format("unknown queue %s", queueUrl));
    }

    return queue.peek();
  }

}
