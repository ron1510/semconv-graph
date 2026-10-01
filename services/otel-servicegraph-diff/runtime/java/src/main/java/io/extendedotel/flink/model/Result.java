package io.extendedotel.flink.model;

/** State transition and its optional public event. */
public record Result(State state, Event event) {}
