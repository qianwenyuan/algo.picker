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
public abstract class BinaryOperation extends Operation {
    
    Operation leftOperand;
    
    Operation rightOperand;
    
    public BinaryOperation() {
        super();
        leftOperand  = null;
        rightOperand = null;
    }
    
    public BinaryOperation(Operation lop, Operation rop) {
        super();
        leftOperand  = lop;
        rightOperand = rop;
    }
    
    public void setLeftOperand(Operation lop) {
        leftOperand  = lop;
    }
    
    public void setRightOperand(Operation rop) {
        rightOperand = rop;
    }
    
    public Operation getLeftOperand() {
        return leftOperand;
    }
    
    public Operation getRightOperand() {
        return rightOperand;
    }
    
    @Override
    public String toString() {
        String retVal = type + " " + leftOperand.toString() + " " + rightOperand.toString();
        if (!cond.equals("")) {
            retVal += " " + cond;
        }
        return "(" + retVal + ")";
    }
    
}
