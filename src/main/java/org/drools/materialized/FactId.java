package org.drools.materialized;

import java.util.Objects;

public class FactId {
    private final String table;
    private final int id;

    public FactId(String table, int id) {
        this.table = table;
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FactId factId = (FactId) o;
        return id == factId.id && table.equals(factId.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, id);
    }
}
