package io.github.jad_salem.pulse.processor.model;

/** Running aggregate of one session window. Flink POJO. */
public class SessionAccumulator {
    public String sessionId;
    public String userId;
    public long firstEventTime = Long.MAX_VALUE;
    public long lastEventTime = Long.MIN_VALUE;
    public int eventCount;
    public int pageViews;
    public int productViews;
    public int addToCarts;
    public int checkouts;
    public int purchases;
    public long revenueCents;
    public String country;
    public String device;
    public String landingPage;

    public SessionAccumulator() {}
}
