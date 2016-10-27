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
public abstract class UnaryOperation extends Operation {
    
    Operation operand;
    
    public UnaryOperation() {
        super();
        operand = null;
    }
    
    public UnaryOperation(Operation op) {
        operand = op;
    }
    
    public void setOperand(Operation op) {
        operand = op;
    }
    
    public Operation getOperand() {
        return operand;
    }
    
    @Override
    public String toString() {
        String retVal = "";
        if (operand == null) {
            retVal += arg;
        } else {
            retVal += type + " " + operand.toString();
            if (!cond.equals("")) {
                retVal += " " + cond;
            }
        }
        return "(" + retVal + ")";
    }
    
}
