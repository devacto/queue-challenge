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
        queueService.push('unknown-queue', 'Test Message')

        fail("expected IOException for unknown queue $unknownQueue")
    }

    @Test
    void "correctly inserts a payload into a queue if the right url is given"() {
        queueService.push(TEST_QUEUE_URL, 'Test Message')
        BlockingQueue<Message> messageQueue = queueMap.get(TEST_QUEUE_URL)

        assert messageQueue.poll().payload == 'Test Message'
    }
}
