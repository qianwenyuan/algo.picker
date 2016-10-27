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
public abstract class Operation {
    
    String type;
    
    String arg;
    
    String cond;
    
    public Operation() {
        type = "";
        arg  = "";
        cond = "";
    }
    
    public abstract String operate();
    
    public String getType() {
        return type;
    }
    
    public void setType(String t) {
        type = t;
    }
    
    public String getArg() {
        return arg;
    }
    
    public void setArg(String a) {
        arg = a;
    }
    
    public String getCond() {
        return cond;
    }
    
    public void setCond(String c) {
        cond = c;
    }
    
}
