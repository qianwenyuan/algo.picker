package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.CanProduce;
import fdu.service.operation.SqlOperation;
import fdu.service.operation.UnaryOperation;
import fdu.util.UserSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Objects;

/**
 * Created by slade on 2016/11/23.
 */
public class Project extends UnaryOperation implements SqlOperation {
    private final String projections;

    public Project(String name, String type, String projections) {
        super(name, type);
        this.projections = projections;
    }

    public static Project newInstance(JSONObject obj) {
        return new Project(
                obj.getString("name"),
                obj.getString("type"),
                obj.getString("projections"));
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getChild().accept(visitor);
        visitor.visitProject(this);
    }

    public String getProjections() {
        return projections;
    }

    public String getFormattedProjections() {
        if ("*".equals(projections)) {
            throw new AssertionError("* is not supported yet, you should specify all column name");
        }

        String[] cols = projections.split(",");
        StringBuilder result = new StringBuilder(projections.length());
        for (String col : cols) {
            result.append("\"");
            result.append(col.trim());
            result.append("\"");
            result.append(",");
        }
        return result.substring(0, result.length() - 1);
    }

    private String[] getProjectionList() {
        return Arrays
                .stream(projections.split(","))
                .map(String::trim)
                .toArray(String[]::new);
    }

    private boolean isProjectAll() {
        return projections.trim().equals("*");
    }

    @Override
    public String toString() {
        return "([Project: " + projections + "] " + getChild() + ")";
    }

    @Override
    public String toSql() {
        return "SELECT " + projections + " FROM " + ((SqlOperation) getChild()).toSql();
    }

    @Override
    public Dataset<Row> execute(UserSession session) {
        Dataset<Row> child = ((CanProduce<Dataset<Row>>) getChild())
                .executeCached(session);
        if (isProjectAll()) {
            return child;
        } else {
            String[] cols = getProjectionList();
            return child.select(cols[0], Arrays.copyOfRange(cols, 1, cols.length));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Project project = (Project) o;
        return Objects.equals(projections, project.projections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), projections);
    }
}
