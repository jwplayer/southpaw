package com.jwplayer.southpaw.strategy;

import org.junit.Test;

import static org.junit.Assert.*;

public class QueueingStrategyTest {
    @Test
    public void testConfigure() {
        QueueingStrategy strategy = new QueueingStrategy();
        strategy.configure(null);
    }

    @Test
    public void testGetPriority() {
        QueueingStrategy strategy = new QueueingStrategy();
        assertEquals(QueueingStrategy.Priority.MEDIUM, strategy.getPriority(null, null, null));
    }
}
