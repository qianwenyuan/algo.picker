package fdu.service;

import fdu.service.operation.Operation;

public class Job {
    private String jid;
    private String table;
    private Operation rootOperation;

    public Job(String jid, String table, Operation rootOperation) {
        this.jid = jid;
        this.table = table;
        this.rootOperation = rootOperation;
    }

    public String getJid() {
        return jid;
    }

    public Operation getRootOperation() {
        return rootOperation;
    }

    public String getTable() {
        return table;
    }
}
