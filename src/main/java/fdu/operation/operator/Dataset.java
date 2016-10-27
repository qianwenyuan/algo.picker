/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.operation.operator;

import fdu.operation.Operation;
import fdu.operation.UnaryOperation;

/**
 *
 * @author Lu Chang
 */
public class Dataset extends UnaryOperation {

    public Dataset() {
        super();
    }

    public Dataset(Operation op) {
        super(op);
    }
    
    @Override
    public String operate() {
        return null;
    }
    
}
