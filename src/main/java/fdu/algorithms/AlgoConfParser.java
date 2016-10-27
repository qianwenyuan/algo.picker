/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.algorithms;

import java.util.List;

import org.json.*;

import fdu.algorithms.AlgoFactory;
import fdu.input.DataSet;
import fdu.input.FileReadContent;
import fdu.operation.BinaryOperation;
import fdu.operation.Operation;
import fdu.operation.OperationTree;
import fdu.operation.UnaryOperation;

/**
 *
 * @author Lu Chang
 */
public class AlgoConfParser implements AlgoFactory {
    
    private static final String basePackage = "fdu.operation.operator.";
     
    AlgoFactory algoFactory;
    
    String conf;
    
    private String algoType;
    
    private String algoName;
    
    private List<Object> params;
    
    private OperationTree operationTree;
    
    public AlgoConfParser(String confPath) throws Exception {
        conf = FileReadContent.fileReadContent(confPath);
        operationTree = new OperationTree();
    }
    
    public void parse() throws Exception {
        JSONObject confJSON = new JSONObject(conf);
        
        algoType = confJSON.getString("type");
        algoName = confJSON.getString("algo");
        params   = confJSON.getJSONArray("params").toList();
        operationTree.setOperation(parseOperation(confJSON.getJSONObject("left")));
        
    }
    
    private Operation parseOperation(JSONObject json) throws Exception {
        String type = json.getString("type");
        
        Operation operation = (Operation) Class.forName(basePackage + type).newInstance();
        BinaryOperation bo;
        UnaryOperation uo;
        
        JSONObject rObject;
        JSONObject lObject;
        
        operation.setType(type);
        try {
            String cond = json.getString("cond");
            operation.setCond(cond);
        } catch(JSONException e) {
            
        }
        
        try {
            lObject = json.getJSONObject("left");
            
            try {
                rObject = json.getJSONObject("right");
                
                bo = (BinaryOperation) operation;
                bo.setLeftOperand(parseOperation(lObject));
                bo.setRightOperand(parseOperation(rObject));
                
                return bo;
            } catch(JSONException e) {
                uo = (UnaryOperation) operation;
                uo.setOperand(parseOperation(lObject));
                
                return uo;
            }
        } catch(JSONException e) {
            uo = (UnaryOperation) operation;
            uo.setArg(json.getString("arg"));
            
            return uo;
        }
        
        
    }

    @Override
    public Algo getAlgo() {
        return null;
    }

    @Override
    public DataSet getDataSet(String confData) {
        return null;
    }

    @Override
    public Params getParams(String confData) {
        return null;
    }

    @Override
    public OperationTree getOperationTree() {
        return operationTree;
    }
    
}
