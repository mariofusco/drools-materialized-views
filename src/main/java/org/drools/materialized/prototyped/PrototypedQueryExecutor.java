package org.drools.materialized.prototyped;

import java.util.Map;

import org.drools.core.facttemplates.Fact;
import org.drools.materialized.QueryExecutor;
import org.drools.model.Prototype;
import org.kie.api.runtime.KieSession;

import static org.drools.modelcompiler.facttemplate.FactFactory.createMapBasedFact;

public class PrototypedQueryExecutor extends QueryExecutor<Fact> {

    private PrototypedQueryGenerator queryGenerator;

    public PrototypedQueryExecutor(String query) {
        super(query);
    }

    @Override
    protected KieSession query2KieSession(String query) {
        this.queryGenerator = new PrototypedQueryGenerator();
        return queryGenerator.query2KieSessionViaExecModelDSL(query);
    }

    @Override
    protected Fact createFact(Map<String, Object> map, String table) {
        Prototype prototype = queryGenerator.getPrototype(table);
        Fact fact = createMapBasedFact( prototype );
        updateFact(map, fact);
        return fact;
    }

    @Override
    protected void updateFact(Map<String, Object> map, Fact fact) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            fact.set(entry.getKey(), entry.getValue());
        }
    }
}
