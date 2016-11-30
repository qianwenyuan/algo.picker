/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation;

/**
 *
 * @author Lu Chang
 */
public abstract class BinaryOperation extends Operation {
    
    Operation left;
    Operation right;

    public BinaryOperation(String id, String type, String z) {
        super(id, type, z);
    }

    public void setLeft(Operation left) {
        this.left = left;
    }

    public void setRight(Operation right) {
        this.right = right;
    }

    public Operation getLeft() {
        return left;
    }

    public Operation getRight() {
        return right;
    }
}
