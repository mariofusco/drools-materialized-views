package org.drools.materialized.prototyped;

import java.util.function.Function;

import org.drools.model.Model;
import org.drools.model.Prototype;
import org.drools.model.Query;
import org.drools.model.impl.ModelImpl;
import org.drools.modelcompiler.builder.KieBaseBuilder;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

public class PrototypedQueryGenerator {

    private static final boolean USE_ALGEBRA = true;

    private final PrototypeFactory prototypeFactory = new PrototypeFactory();

    private final Function<String, Query> queryTransformer = USE_ALGEBRA ?
            new AlgebraTransformer(prototypeFactory) :
            new AstTransformer(prototypeFactory);

    public KieSession query2KieSessionViaExecModelDSL(String query) {
         Query droolsQuery = toDroolsQuery(query);

        Model model = new ModelImpl().addQuery( droolsQuery );
        KieBase kieBase = KieBaseBuilder.createKieBaseFromModel( model );
        return kieBase.newKieSession();
    }

    private Query toDroolsQuery(String query) {
        return queryTransformer.apply(query);
    }

    public Prototype getPrototype(String name) {
        return prototypeFactory.getPrototype(name);
    }
}
