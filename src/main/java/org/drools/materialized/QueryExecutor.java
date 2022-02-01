package org.drools.materialized;

import java.util.HashMap;
import java.util.Map;

import org.drools.core.common.InternalFactHandle;
import org.drools.modelcompiler.ExecutableModelProject;
import org.json.JSONObject;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.ViewChangedEventListener;
import org.kie.internal.utils.KieHelper;

import static org.drools.materialized.QueryParser.queryToPkgDescr;

public class QueryExecutor {

    private final KieSession ksession;

    private final Map<FactId, FactHandle> factMap = new HashMap<>();

    private QueryExecutor(KieSession ksession) {
        this.ksession = ksession;
    }

    public static QueryExecutor create(String query) {
        KieSession ksession = new KieHelper()
                .addContent( queryToPkgDescr(query) )
                .build(ExecutableModelProject.class)
                .newKieSession();

        return new QueryExecutor(ksession);
    }


    public void listen(ViewChangedEventListener listener) {
        ksession.openLiveQuery("Q0", new Object[0], listener);
    }

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

    private void insert(int id, Map<String, Object> map, String table) {
        factMap.put(new FactId(table, id), ksession.insert(new Fact(table, map)));
    }

    private void delete(int id, String table) {
        ksession.delete(factMap.remove(new FactId(table, id)));
    }

    private void update(int id, Map<String, Object> map, String table) {
        FactHandle fh = factMap.get(new FactId(table, id));
        Fact fact = (Fact) ((InternalFactHandle) fh).getObject();
        fact.setMap(map);
        ksession.update(fh, fact);
    }

    private Map<String, Object> jsonToMap(JSONObject jsonObject, String key) {
        return jsonObject.isNull(key) ? null : jsonObject.getJSONObject(key).toMap();
    }
}
