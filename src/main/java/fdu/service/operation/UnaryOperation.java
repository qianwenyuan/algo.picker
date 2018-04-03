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
public abstract class UnaryOperation extends Operation {

    private Operation child;

    public UnaryOperation(String name, String type) {
        super(name, type);
    }

    public void setChild(Operation child) {
        this.child = child;
    }

    public Operation getChild() {
        return child;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        UnaryOperation that = (UnaryOperation) o;
        return Objects.equals(child, that.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), child);
    }
}
