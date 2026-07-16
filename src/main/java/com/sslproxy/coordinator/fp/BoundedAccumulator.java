package com.sslproxy.coordinator.fp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public final class BoundedAccumulator<T> {

    private final LinkedBlockingDeque<T> queue;
    private final int capacity;
    private final String name;

    public BoundedAccumulator(String name, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.name = name;
        this.capacity = capacity;
        this.queue = new LinkedBlockingDeque<>(capacity);
    }

    public boolean offer(T item) {
        return queue.offer(item);
    }

    public List<T> drain(int maxItems) {
        if (maxItems <= 0) {
            return List.of();
        }
        List<T> batch = new ArrayList<>(Math.min(maxItems, capacity));
        queue.drainTo(batch, maxItems);
        return Collections.unmodifiableList(batch);
    }

    public List<T> requeueFront(List<T> items) {
        if (items.isEmpty()) {
            return List.of();
        }
        List<T> rejected = new ArrayList<>();
        for (int i = items.size() - 1; i >= 0; i--) {
            T item = items.get(i);
            if (!queue.offerFirst(item)) {
                rejected.add(item);
            }
        }
        Collections.reverse(rejected);
        return Collections.unmodifiableList(rejected);
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    public String name() {
        return name;
    }
}
