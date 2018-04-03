/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation;

import fdu.bean.generator.OperatorVisitor;

import java.util.Objects;

/**
 *
 * @author slade
 *
 */
public abstract class Operation {
    
    protected final String type;
    protected final String name;
    private transient boolean needCache;

    Operation(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public abstract void accept(OperatorVisitor visitor);

    public boolean isNeedCache() {
        return needCache;
    }

    public void setNeedCache() {
        this.needCache = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return Objects.equals(type, operation.type) &&
                Objects.equals(name, operation.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }
}
