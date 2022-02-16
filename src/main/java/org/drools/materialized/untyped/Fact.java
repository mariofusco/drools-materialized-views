package org.drools.materialized.untyped;

import java.util.Map;

public class Fact {

    private final String table;
    private Map<String, Object> map;

    public Fact(String table, Map<String, Object> map) {
        this.table = table;
        this.map = map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    public String getTable() {
        return table;
    }

    public Object get(String key) {
        return map.get(key);
    }

    @Override
    public String toString() {
        return table + ": " + map;
    }
}
