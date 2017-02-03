package io.github.rynffoll.executor

import spock.lang.Specification

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit


class ThreadPoolWithTimeoutExecutorSpec extends Specification {

    def "Work threads are interrupted by timeout"() {
        setup:
        println "$queueSize | $threads | $timeout | $delay | $interrupted | $delayBeforeSubmit"

        def workQueue = new ArrayBlockingQueue(queueSize)
        def threadPool = new ThreadPoolWithTimeoutExecutor(timeout, threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue);

        def range = (1..queueSize)
        def tasks = generateWorkThreads(queueSize, delay)
        when:
        def futures = new ArrayList<Future<Boolean>>()
        range.each {
            futures << threadPool.submit(tasks.get(it - 1))
            if (delayBeforeSubmit) {
                sleep(generateRandomDelay())
            }
        }
        while (!futures.every { it.isDone() }) {}
        then:
        futures.eachWithIndex { e, i ->
            if (e.get() != interrupted) {
                println "${System.currentTimeMillis()}: ${i}/${futures.size() - 1}) ${e} / ${e.get() ? 'interrupt' : 'done'}"
            }
        }
        futures.every {
            it.get() == interrupted
        }
        where:
        queueSize | threads | timeout | delay | interrupted | delayBeforeSubmit
        1000      | 100     | 1000    | 100   | false       | false
        1000      | 100     | 100     | 1000  | true        | false
        100       | 10      | 1000    | 100   | false       | true
        100       | 10      | 100     | 1000  | true        | true
    }

    def "Correct end of work after shutdown"() {
        setup:
        println "$queueSize | $threads | $timeout | $delay | $interrupted | $delayBeforeSubmit"

        def workQueue = new ArrayBlockingQueue(queueSize)
        def threadPool = new ThreadPoolWithTimeoutExecutor(timeout, threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue);

        def range = (1..queueSize)
        def tasks = generateWorkThreads(queueSize, delay)
        when:
        def futures = new ArrayList<Future<Boolean>>()
        range.each {
            futures << threadPool.submit(tasks.get(it - 1))
            if (delayBeforeSubmit) {
                sleep(generateRandomDelay())
            }
        }
        sleep(100)
        threadPool.shutdown()
        while (!futures.every { it.isDone() }) {}
        sleep(1_000)
        then:
        futures.eachWithIndex { e, i ->
            if (e.get() != interrupted) {
                println "${System.currentTimeMillis()}: ${i}/${futures.size() - 1}) ${e} / ${e.get() ? 'interrupt' : 'done'}"
            }
        }
        futures.every {
            it.get() == interrupted
        }
        where:
        queueSize | threads | timeout | delay | interrupted | delayBeforeSubmit
        1000      | 100     | 1000    | 100   | false       | false
        1000      | 100     | 100     | 1000  | true        | false
        100       | 10      | 1000    | 100   | false       | true
        100       | 10      | 100     | 1000  | true        | true
    }

    def generateWorkThreads(int size, long delay) {
        def tasks = new ArrayList<Runnable>()
        (1..size).each {
            tasks << ([
                    call: {
                        def isInterrupted = false
                        try {
                            Thread.sleep(delay)
                        } catch (Exception e) {
                            isInterrupted = true
                        }
                        return isInterrupted
                    }
            ] as Callable)
        }
        tasks
    }

    def generateRandomDelay() {
        Math.abs(new Random().nextInt() % 10) as long
    }
}
