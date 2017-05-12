package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.CanProduceDataFrame;
import fdu.service.operation.SqlOperation;
import fdu.service.operation.UnaryOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import java.util.Arrays;

/**
 * Created by slade on 2016/11/23.
 */
public class Project extends UnaryOperation implements SqlOperation {
    private  String name;
    private String projections;

    public Project(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitProject(this);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setProjections(String projections) {
        this.projections = projections;
    }

    public String getName() {
        return name;
    }

    public String getProjections() {
        return projections;
    }

    public String getFormattedProjections(){
        if ("*".equals(projections)){
            throw new AssertionError("* is not supported yet, you should specify all column name");
        }

        String[] cols = projections.split(",");
        StringBuilder result = new StringBuilder(projections.length());
        for (String col : cols){
            result.append("\"");
            result.append(col.trim());
            result.append("\"");
            result.append(",");
        }
        return result.substring(0, result.length()-1);
    }

    public static Project newInstance(JSONObject obj){
        Project result = new Project(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setProjections(obj.getString("projections"));
        return result;
    }

    @Override
    public String toString() {
        return "([Project: " + projections +"] "+ getLeft() +")";
    }

    @Override
    public String toSql() {
        return "SELECT " + projections + " FROM " + ((SqlOperation)getLeft()).toSql();
    }

    @Override
    public Dataset<Row> execute(SparkSession spark) {
        String[] cols = Arrays
                .stream(projections.split(","))
                .map(String::trim)
                .toArray(String[]::new);
        return ((CanProduceDataFrame)getLeft())
                .execute(spark)
                .select(cols[0], Arrays.copyOfRange(cols, 1, cols.length));
    }
}
