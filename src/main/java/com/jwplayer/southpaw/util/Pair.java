package com.jwplayer.southpaw.util;

/**
 * Simple pair class
 */
public class Pair<A, B> {
    protected final A a;
    protected final B b;

    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }

    public A getA() {
        return a;
    }

    public B getB() {
        return b;
    }

    @Override
    public String toString() {
        return String.format("{a=%s, b=%s}", a, b);
    }
}
