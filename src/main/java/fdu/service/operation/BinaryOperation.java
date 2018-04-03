/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation;

import java.util.Objects;

/**
 *
 * @author Lu Chang
 */
public abstract class BinaryOperation extends Operation {
    
    private Operation left;
    private Operation right;

    public BinaryOperation(String name, String type) {
        super(name, type);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        BinaryOperation that = (BinaryOperation) o;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), left, right);
    }
}
