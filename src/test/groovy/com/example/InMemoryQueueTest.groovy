package com.example

import java.util.concurrent.BlockingQueue

import static junit.framework.TestCase.fail

import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

import java.util.concurrent.ArrayBlockingQueue

@CompileStatic
class InMemoryQueueTest {

    private static final String TEST_QUEUE_URL = 'test-queue'
    private static final String TEST_PAYLOAD = 'test-message'

    private InMemoryQueueService queueService

    private Map<String, ArrayBlockingQueue<Message>> queueMap

    @Before
    void setup() {
        this.queueMap = [:]
        queueMap.put(TEST_QUEUE_URL, new ArrayBlockingQueue<Message>(10))

        this.queueService = new InMemoryQueueService(queueMap)
    }

    @Test(expected = IOException)
    void "throws an exception if a message is pushed into an unknown queue"() {
        String unknownQueue = 'unknown-queue'
        queueService.push('unknown-queue', TEST_PAYLOAD)

        fail("expected IOException for unknown queue $unknownQueue")
    }

    @Test
    void "inserts a payload into a queue if the right url is given"() {
        queueService.push(TEST_QUEUE_URL, TEST_PAYLOAD)
        BlockingQueue<Message> messageQueue = queueMap.get(TEST_QUEUE_URL)

        assert messageQueue.poll().payload == TEST_PAYLOAD
    }

    @Test
    void "pushing and retrieving a payload from a queue given the right url"() {
        queueService.push(TEST_QUEUE_URL, TEST_PAYLOAD)

        Message m = queueService.pull(TEST_QUEUE_URL)
        assert m.payload == TEST_PAYLOAD
    }

    @Test(expected = IOException)
    void "throws an exception when pulling from a non-existent queue"() {
        String unknownQueue = 'unknown-queue'
        queueService.pull(unknownQueue)

        fail("expected IOException for unknown queue $unknownQueue")
    }

    @Test
    void "pull does not remove message from the queue"() {
        queueService.push(TEST_QUEUE_URL, TEST_PAYLOAD)
        queueService.pull(TEST_QUEUE_URL)

        assert queueMap.size() == 1
    }

}
