/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service;

import fdu.Conf;
import fdu.service.operation.BinaryOperation;
import fdu.service.operation.Operation;
import fdu.service.operation.UnaryOperation;
import fdu.service.operation.operators.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author slade
 */
@Service
public class OperationParserService {

    Map<String, OpType> opTypeMap = new HashMap<>();

    public OperationParserService() throws IOException {
        opTypeMap.put(Conf.DATASOURCE, OpType.DATASOURCE);
        opTypeMap.put(Conf.FILTER, OpType.FILTER);
        opTypeMap.put(Conf.JOIN, OpType.JOIN);
        opTypeMap.put(Conf.PROJECT, OpType.PROJECT);
        opTypeMap.put(Conf.KMEANS_MODEL, OpType.KMEANS_MODEL);
    }

    private String confWrapper(String conf) {
        return "{ \"infos\": " + conf + "}";
    }

    public Operation parse(String conf) {
        JSONObject confJSON = new JSONObject(confWrapper(conf));
        return parseOperation(confJSON);
    }

    enum OpType {
        KMEANS_MODEL,
        FILTER,
        JOIN,
        DATASOURCE,
        PROJECT
    }

    private Operation parseOperation(JSONObject json) {
        JSONArray infos = (JSONArray) json.get("infos");
        Map<String, Operation> operations = convert2Operations(infos);

        return constructOpTree(infos, operations);
    }

    private Operation constructOpTree(JSONArray infos, Map<String, Operation> operations) {
        Operation root = null;
        for (Object obj : infos) {
            if (!(obj instanceof JSONObject)) {
                throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of " + JSONObject.class.getName());
            }
            if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json object
                continue;
            }

            if (isRoot((JSONObject) obj)) {
                root = getOperationById(operations, (JSONObject) obj);
            } else {
                Operation parent = operations.get(((JSONObject) obj).getJSONArray("wires").getJSONArray(0).getString(0));
                if (parent instanceof UnaryOperation) {
                    if (((UnaryOperation) parent).getLeft() != null) {
                        throw new AssertionError("Child already specified.");
                    } else {
                        ((UnaryOperation) parent).setLeft(getOperationById(operations, (JSONObject) obj));
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
        }

        return root;
    }

    private Operation getOperationById(Map<String, Operation> operations, JSONObject obj) {
        return operations.get(obj.getString("id"));
    }

    private boolean isRoot(JSONObject obj) {
        return obj.getJSONArray("wires").length() == 0;
    }

    private Map<String, Operation> convert2Operations(JSONArray infos) {
        Map<String, Operation> result = new HashMap<>();
        for (Object obj : infos) {
            if (!(obj instanceof JSONObject)) {
                throw new AssertionError("converting fails: obj is of type " + obj.getClass().getName() + " instead of " + JSONObject.class.getName());
            }
            if ("tab".equals(((JSONObject) obj).get("type"))) { // skip tab json object
                continue;
            }
            Operation op = convert((JSONObject) obj);
            result.put(op.getId(), op);
        }
        return result;
    }

    //TODO: use reflection instead of string enum mapping.
    private Operation convert(JSONObject obj) {
        Operation result;
        OpType type = opTypeMap.get(obj.getString("type"));
        switch (type) {
            case DATASOURCE:
                result = DataSource.newInstance(obj);
                break;
            case JOIN:
                result = Join.newInstance(obj);
                break;
            case FILTER:
                result = Filter.newInstance(obj);
                break;
            case PROJECT:
                result = Project.newInstance(obj);
                break;
            case KMEANS_MODEL:
                result = KMeansModel.newInstance(obj);
                break;
            default:
                throw new AssertionError("type " + obj.get("type") + " not exists");
        }
        return result;
    }
}
