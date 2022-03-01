package org.drools.materialized.prototyped;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.Query;

import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;

public abstract class AbstractSqlTransformer implements Function<String, Query>  {

    private final PrototypeFactory prototypeFactory;

    protected final Map<String, PrototypeVariable> variables = new HashMap<>();

    public AbstractSqlTransformer(PrototypeFactory prototypeFactory) {
        this.prototypeFactory = prototypeFactory;
    }

    protected PrototypeDSL.PrototypePatternDef createPrototypePatternDef(String table, String id) {
        PrototypeVariable protoVar = variable(prototypeFactory.getPrototype(table), id);
        variables.put(id, protoVar);
        return protoPattern( protoVar );
    }
}
