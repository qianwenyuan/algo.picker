/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation;

import fdu.bean.generator.OperatorVisitor;

/**
 *
 * @author slade
 *
 */
public abstract class Operation {
    
    private String id;
    private String type;
    private String z;

    public Operation(String id, String type, String z) {
        this.id = id;
        this.type = type;
        this.z = z;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getZ() {
        return z;
    }

    public abstract void accept(OperatorVisitor visitor);
}
