package io.github.rynffoll.executor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;


/**
 * Thread pool executor implementation that supervises their threads
 * using separate supervisor thread.
 * Supervisor thread interrupt submitted thread after timeout.
 *
 * @author Ruslan Kamashev
 */
public class ThreadPoolWithTimeoutExecutor extends ThreadPoolExecutor {

  public static final Logger logger = LoggerFactory.getLogger(ThreadPoolWithTimeoutExecutor.class);

  private ThreadPoolExecutor supervisorThreadPool;
  private final DelayQueue<SupervisedThread> supervisedThreadsQueue = new DelayQueue<>();
  private final Set<Runnable> onlineThreads = Collections.newSetFromMap(new ConcurrentHashMap<Runnable, Boolean>());
  private long timeout;

  /**
   * {@inheritDoc}
   *
   * @param timeout- timeout in milliseconds
   */
  public ThreadPoolWithTimeoutExecutor(long timeout,
                                       int corePoolSize,
                                       int maximumPoolSize,
                                       long keepAliveTime,
                                       TimeUnit unit,
                                       BlockingQueue workQueue) {
    this(timeout, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, Executors.defaultThreadFactory());
  }

  /**
   * {@inheritDoc}
   *
   * @param timeout - timeout in milliseconds
   */
  public ThreadPoolWithTimeoutExecutor(long timeout,
                                       int corePoolSize,
                                       int maximumPoolSize,
                                       long keepAliveTime,
                                       TimeUnit unit,
                                       BlockingQueue workQueue,
                                       ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    init(timeout);
  }

  /**
   * Do some initialization work
   *
   * @param timeout - timeout in milliseconds
   */
  private void init(long timeout) {
    this.timeout = timeout;

    supervisorThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1),
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("supervisor-" + t.getName());
                return t;
              }
            });
  }

  @Override
  public void shutdown() {
    super.shutdown();
    shutdownSupervisor();
  }

  /**
   * Clear resources are used by supervisor thread
   */
  private void shutdownSupervisor() {
    if (logger.isTraceEnabled()) {
      logger.trace("Shutdown supervisor thread: online threads = {}", onlineThreads.size());
    }
    supervisorThreadPool.shutdownNow(); // interrupt supervisor thread
  }

  /**
   * Add work thread to set {@link #onlineThreads},
   * add work thread wrapper {@link SupervisedThread} to queue {@link #supervisedThreadsQueue}
   * {@inheritDoc}
   */
  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    synchronized (r) {
      onlineThreads.add(r);
      supervisedThreadsQueue.put(new SupervisedThread(r, t, System.currentTimeMillis()));
      synchronized (supervisorThreadPool) { // fix RejectedExecutionException
        if (supervisorThreadPool.getPoolSize() == 0 && !supervisorThreadPool.isShutdown()) {
          supervisorThreadPool.submit(new SupervisorRunnable());
        }
      }

      super.beforeExecute(t, r);
    }
  }

  /**
   * Remove work thread from set {@link #onlineThreads}
   * {@inheritDoc}
   */
  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    synchronized (r) {
      super.afterExecute(r, t);
      onlineThreads.remove(r);
    }
  }

  /**
   * Wrapper for work thread which is monitored by supervisor
   */
  private class SupervisedThread implements Delayed {
    private final Runnable runnable;
    private final Thread thread;
    private final long startTime;

    public SupervisedThread(Runnable runnable, Thread thread, long startTime) {
      this.runnable = runnable;
      this.thread = thread;
      this.startTime = startTime;
    }

    @Override
    public String toString() {
      return "SupervisedThread{" +
              "runnable=" + runnable +
              ", thread=" + thread +
              ", startTime=" + startTime +
              '}';
    }

    public Thread getThread() {
      return thread;
    }

    public long getStartTime() {
      return startTime;
    }

    public Runnable getRunnable() {
      return runnable;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      // DelayQueue uses TimeUnit.NANOSECONDS
      return unit.MILLISECONDS.toNanos((timeout - (System.currentTimeMillis() - startTime)));
    }

    @Override
    public int compareTo(Delayed o) {
      if (this.getDelay(TimeUnit.NANOSECONDS) < o.getDelay(TimeUnit.NANOSECONDS))
        return -1;
      if (this.getDelay(TimeUnit.NANOSECONDS) > o.getDelay(TimeUnit.NANOSECONDS))
        return 1;
      return 0;
    }
  }

  /**
   * Supervisor thread which watches work threads
   */
  private class SupervisorRunnable implements Runnable {

    @Override
    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("Supervisor thread was started");
      }
      boolean isInterrupted = false;
      while (true) {
        try {
          SupervisedThread thread = supervisedThreadsQueue.poll(timeout, TimeUnit.MILLISECONDS);
          if (thread != null) {
            Runnable runnable = thread.getRunnable();
            synchronized (runnable) {
              if (onlineThreads.contains(runnable)) {
                thread.getThread().interrupt();
              }
            }
          }
          if (isInterrupted && onlineThreads.isEmpty()) {
            if (logger.isTraceEnabled()) {
              logger.trace("Supervisor thread finished: online threads =  {} / supervisor queue = {}", onlineThreads.size(), supervisedThreadsQueue.size());
            }
            supervisedThreadsQueue.clear();
            break;
          }
        } catch (InterruptedException e) {
          logger.trace("Supervisor thread got interrupt signal");
          isInterrupted = true;
        }
      }
    }
  }
}
