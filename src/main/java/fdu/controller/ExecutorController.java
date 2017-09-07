package fdu.controller;

import fdu.Config;
import fdu.service.Job;
import fdu.service.OperationParserService;
import fdu.service.operation.CanProduce;
import fdu.service.operation.Operation;
import fdu.util.ResultSerialization;
import fdu.util.UserSession;
import fdu.util.UserSessionPool;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liangchg on 2017/4/6.
 */
@RestController
public class ExecutorController {

    private final OperationParserService operationParserService;

    @Autowired
    public ExecutorController(OperationParserService operationParserService) {
        this.operationParserService = operationParserService;

    }

    private UserSession getUserSession(HttpServletRequest request) throws IOException {
        return UserSessionPool.getInstance().addOrGetUserSession(request.getSession().getId());
    }

    @RequestMapping(value = "/node", method = RequestMethod.POST)
    public String generateDriver(@RequestBody String conf, @Autowired HttpServletRequest request) throws IOException {
        UserSession userSession = getUserSession(request);
        Config.setAddress(request.getRemoteAddr());
        new Thread(() -> {
            long start = System.currentTimeMillis();
            Job job = operationParserService.parse(conf);
            Object res = ((CanProduce<Dataset<Row>>) job.getRootOperation()).executeCached(userSession);
            if (res != null) {
                ((Dataset) res).write().saveAsTable(job.getTable());
            }
            try {
                userSession.sendResult(Config.getAddress(), job.getJid(), ResultSerialization.toString(res));
                System.out.println(ResultSerialization.toString(res));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(System.currentTimeMillis() - start);
        }).start();
        return "OK";
    }

    @Deprecated
    @RequestMapping(value = "/run", method = RequestMethod.POST)
    public String executeCommand(@RequestBody String code, @Autowired HttpServletRequest request) throws IOException {
        UserSession userSession = getUserSession(request);
        new Thread(() -> {
//            try {
//                Object result = userSession.getEmbeddedExecutor().eval(code);
//                userSession.sendResult(result == null ? null : result.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }).start();
        return "OK";
    }

    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public String getTables(@Autowired HttpServletRequest request) throws IOException {
        UserSession userSession = getUserSession(request);
        return new JSONArray(userSession.getEmbeddedExecutor().getTableNames()).toString();
    }

    @RequestMapping(value = "/schemas", method = RequestMethod.POST)
    public String getTableMeta(@RequestBody String tableNames, @Autowired HttpServletRequest request) throws IOException {
        UserSession userSession = getUserSession(request);
        JSONArray array = new JSONArray(tableNames);
        List<String> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            list.add(array.getString(i));
        }
        String[] tables = list.toArray(new String[list.size()]);
        Tuple2<String, String>[] schemas = userSession.getEmbeddedExecutor().getTableSchemas(tables);
        JSONArray result = new JSONArray();
        for (Tuple2<String, String> schema: schemas) {
            JSONObject obj = new JSONObject();
            obj.put("tableName", schema._1());
            obj.put("schema", new JSONObject(schema._2()));
            result.put(obj);
        }
        return result.toString();
    }
}