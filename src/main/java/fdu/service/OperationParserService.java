/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service;

import fdu.OperationNodeConfiguration;
import fdu.service.operation.BinaryOperation;
import fdu.service.operation.Operation;
import fdu.service.operation.UnaryOperation;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author slade
 */
@Service
public class OperationParserService {

    private String confWrapper(String conf) {
        return "{ \"infos\": " + conf + "}";
    }

    public Job parse(String conf) {
        JSONObject confJSON = new JSONObject(confWrapper(conf));
        return parseOperation(confJSON);
    }

    private Job parseOperation(JSONObject json) {
        JSONObject infos = (JSONObject) json.get("infos");
        String jid = infos.getString("jid");
        String table = infos.getString("table");
        JSONArray data = infos.getJSONArray("data");
        Map<String, Operation> operations = convert2Operations(data);

        Operation op = constructOpTree(data, operations);
        return new Job(jid, table, op);
    }

    private Operation constructOpTree(JSONArray infos, Map<String, Operation> operations) {
        Operation root = null;
        for (int n = 0; n < infos.length(); n++) {
            Object obj = infos.get(n);
            if (!(obj instanceof JSONObject)) {
                throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of " + JSONObject.class.getName());
            }
            if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json object
                continue;
            }
            // TODO: multiple Roots?
            if (isRoot((JSONObject) obj)) {
                root = getOperationById(operations, (JSONObject) obj);
                root.setNeedCache();
            } else {
                int parentNum = ((JSONObject) obj).getJSONArray("wires").getJSONArray(0).length();
                if (parentNum > 1)
                    operations.get(((JSONObject) obj).getString("id")).setNeedCache();
                for (int i = 0; i < parentNum; i++) {
                    Operation parent = operations.get(((JSONObject) obj).getJSONArray("wires").getJSONArray(0).getString(i));
                    setupParents(parent, operations, obj);
                }
            }
        }
        return root;
    }

    private void setupParents(Operation parent, Map<String, Operation> operations, Object obj) {
        if (parent instanceof UnaryOperation) {
            if (((UnaryOperation) parent).getChild() != null) {
                throw new AssertionError("Child already specified.");
            } else {
                ((UnaryOperation) parent).setChild(getOperationById(operations, (JSONObject) obj));
            }
        } else if (parent instanceof BinaryOperation) {
            if (((BinaryOperation) parent).getLeft() != null && ((BinaryOperation) parent).getRight() != null) {
                throw new AssertionError("Both children are specified");
            }

            if (((BinaryOperation) parent).getLeft() == null) {
                ((BinaryOperation) parent).setLeft(getOperationById(operations, (JSONObject) obj));
            } else {
                ((BinaryOperation) parent).setRight(getOperationById(operations, (JSONObject) obj));
            }

        }
    }

    private Operation getOperationById(Map<String, Operation> operations, JSONObject obj) {
        return operations.get(obj.getString("id"));
    }

    private boolean isRoot(JSONObject obj) {
        return obj.getJSONArray("wires").length() == 0 || obj.getJSONArray("wires").getJSONArray(0).length() == 0;
    }

    private Map<String, Operation> convert2Operations(JSONArray infos) {
        Map<String, Operation> result = new HashMap<>();
        for (int i = 0; i < infos.length(); i++) {
            Object obj = infos.get(i);
            if (!(obj instanceof JSONObject)) {
                throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of " + JSONObject.class.getName());
            }
            if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json object
                continue;
            }
            Operation op = convert((JSONObject) obj);
            result.put(((JSONObject) obj).getString("id"), op);
        }
        return result;
    }

    private Operation convert(JSONObject obj) {
        Operation result;
        Class<?> opClass = OperationNodeConfiguration.opMap.get(obj.getString("type"));
        try {
            Method newInstanceMethod = opClass.getMethod("newInstance", JSONObject.class);
            result = (Operation) newInstanceMethod.invoke(null, obj);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AssertionError("type " + obj.get("type") + " not exists:\n" + e.getMessage());
        }
        return result;
    }
}
