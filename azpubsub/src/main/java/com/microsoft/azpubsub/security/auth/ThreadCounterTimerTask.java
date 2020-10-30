package com.microsoft.azpubsub.security.auth;

import java.util.TimerTask;

public class ThreadCounterTimerTask extends TimerTask {
    private String topicName = null;
    private int threadCount = 0;
    private Long ioThreadId = 0L;
    TopicThreadCounter topicThreadCounterInstance = TopicThreadCounter.getInstance(5000L);

    public void setTopicName(String topic) {
        topicName = topic;
    }

    public void setIoThreadId(Long threadId) {
        ioThreadId = threadId;
    }

    public int getThreadCount() {
        return threadCount;
    }

    @Override
    public void run() {
        if(null != topicName) {
            threadCount = topicThreadCounterInstance.add(topicName, System.currentTimeMillis(), ioThreadId);
        }
    }
}
