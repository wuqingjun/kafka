package com.microsoft.azpubsub.security.auth;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TopicThreadCounter {
    private static TopicThreadCounter instance = null;
    private static Object lock = new Object();
    private Long interval = null;
    ConcurrentHashMap<String, TreeMap<Long, Long>> topicThreadMap = new ConcurrentHashMap<>();

    public TopicThreadCounter(Long intvl) {
        interval = intvl;
    }

    static TopicThreadCounter getInstance(Long interval) {
        synchronized (lock) {
            if(null == instance) {
                instance = new TopicThreadCounter(interval);
            }
        }
        return instance;
    }

    public int add(String topic, Long currentTimeInMs, Long threadId) {
        if(!topicThreadMap.containsKey(topic)) {
            topicThreadMap.put(topic, new TreeMap<>());
        }
        topicThreadMap.get(topic).put(currentTimeInMs, threadId);
        NavigableMap<Long, Long> subMap= topicThreadMap.get(topic).tailMap(currentTimeInMs - interval, false);
        HashSet<Long> hs = new HashSet<>();
        for(Map.Entry element: subMap.entrySet()) {
            hs.add((Long)element.getValue());
        }
        topicThreadMap.put(topic, new TreeMap<>(subMap));
        return hs.size();
    }
}
