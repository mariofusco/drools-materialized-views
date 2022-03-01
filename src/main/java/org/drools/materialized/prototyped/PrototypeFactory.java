package org.drools.materialized.prototyped;

import java.util.HashMap;
import java.util.Map;

import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;

public class PrototypeFactory {
    private final Map<String, Prototype> prototypes = new HashMap<>();

    public Prototype getPrototype(String name) {
        return prototypes.computeIfAbsent(name, PrototypeDSL::prototype);
    }
}
