/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.operation.operator;

import fdu.operation.BinaryOperation;
import fdu.operation.Operation;

/**
 *
 * @author Lu Chang
 */
public class Join extends BinaryOperation {
    
    public Join() {
        super();
    }

    public Join(Operation lop, Operation rop) {
        super(lop, rop);
    }
    
    @Override
    public String operate() {
        return null;
    }
    
}
