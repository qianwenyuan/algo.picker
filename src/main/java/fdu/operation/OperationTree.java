/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.operation;

/**
 *
 * @author Lu Chang
 */
public class OperationTree {
    
    Operation operation;
    
    public OperationTree() {
        this.operation = null;
    }
    
    public Operation getOperation() {
        return this.operation;
    }
    
    public void setOperation(Operation op) {
        operation = op;
    }
    
    public String merge() {
        return null;
    }
    
    @Override
    public String toString() {
        return operation.toString();
    }
    
}
