package org.drools.materialized;

import java.util.HashMap;
import java.util.Map;

import org.drools.core.common.InternalFactHandle;
import org.drools.core.facttemplates.Fact;
import org.drools.materialized.prototyped.PrototypedQueryExecutor;
import org.drools.materialized.untyped.UntypedQueryExecutor;
import org.json.JSONObject;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.ViewChangedEventListener;

public abstract class QueryExecutor<T> {

    private static final boolean USE_PROTOTYPES = true;

    protected final KieSession ksession;

    protected final Map<FactId, FactHandle> factMap = new HashMap<>();

    protected QueryExecutor(String query) {
        this.ksession = query2KieSession(query);
    }

    public static QueryExecutor create(String query) {
        return create(query, USE_PROTOTYPES);
    }

    public static QueryExecutor create(String query, boolean prototype) {
        return prototype ? new PrototypedQueryExecutor(query) : new UntypedQueryExecutor(query);
    }

    public void listen(ViewChangedEventListener listener) {
        ksession.openLiveQuery("Q0", new Object[0], listener);
    }

    protected abstract KieSession query2KieSession(String query);

    public void process(String json) {
        JSONObject jsonObject = new JSONObject(json);
        Map<String, Object> source = jsonToMap(jsonObject, "source");
        String table = source.get("table").toString();

        Map<String, Object> before = jsonToMap(jsonObject, "before");
        Map<String, Object> after = jsonToMap(jsonObject, "after");

        if (before == null) {
            insert((int)after.get("id"), after, table);
        } else if (after == null) {
            delete((int)before.get("id"), table);
        } else {
            update((int)before.get("id"), after, table);
        }

        ksession.fireAllRules();
    }

    protected void insert(int id, Map<String, Object> map, String table) {
        factMap.put(new FactId(table, id), ksession.insert( createFact(map, table) ));
    }

    protected abstract T createFact(Map<String, Object> map, String table);

    protected void delete(int id, String table) {
        ksession.delete(factMap.remove(new FactId(table, id)));
    }

    protected void update(int id, Map<String, Object> map, String table) {
        FactHandle fh = factMap.get(new FactId(table, id));
        T fact = (T) ((InternalFactHandle) fh).getObject();
        updateFact(map, fact);
        ksession.update(fh, fact);
    }

    protected abstract void updateFact(Map<String, Object> map, T fact);

    private Map<String, Object> jsonToMap(JSONObject jsonObject, String key) {
        return jsonObject.isNull(key) ? null : jsonObject.getJSONObject(key).toMap();
    }
}
