package org.drools.materialized.untyped;

import java.util.HashMap;
import java.util.Map;

import org.drools.materialized.FactId;
import org.drools.materialized.QueryExecutor;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import static org.drools.materialized.untyped.UntypedQueryGenerator.query2KieSessionViaDescrs;

public class UntypedQueryExecutor extends QueryExecutor<Fact> {

    private final Map<FactId, FactHandle> factMap = new HashMap<>();

    public UntypedQueryExecutor(String query) {
        super(query);
    }

    @Override
    protected KieSession query2KieSession(String query) {
        return query2KieSessionViaDescrs(query);
    }

    @Override
    protected Fact createFact(Map<String, Object> map, String table) {
        return new Fact(table, map);
    }

    @Override
    protected void updateFact(Map<String, Object> map, Fact fact) {
        fact.setMap(map);
    }
}
