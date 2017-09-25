package fdu.controller;

import fdu.Config;
import fdu.service.Job;
import fdu.service.OperationParserService;
import fdu.service.operation.operators.CanProduce;
import fdu.util.ResultSerialization;
import fdu.util.UserSession;
import fdu.util.UserSessionPool;
import org.apache.spark.sql.Dataset;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URL;
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
    @ResponseBody
    public String generateDriver(@RequestBody final String conf, @Autowired HttpServletRequest request) throws IOException {
        final UserSession userSession = getUserSession(request);
        Config.setAddress(request.getRemoteAddr());
        new Thread(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                Job job = operationParserService.parse(conf);
                try {
                    Object res;
                    if (userSession.getEmbeddedExecutor().tableExists(job.getTable())) {
                        res = userSession.getSparkSession().table(job.getTable());
                    } else {
                        res = ((CanProduce) job.getRootOperation()).executeCached(userSession);
                        if (res != null && res instanceof Dataset) {
                            ((Dataset) res).write().saveAsTable(job.getTable());
                        }
                    }
                    userSession.sendResult(Config.getAddress(), job.getJid(), ResultSerialization.toString(res));
                    System.out.println("Job Finished");
                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "ok"));
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "error"));
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
                System.out.println(System.currentTimeMillis() - start);
            }
        }).start();
        return "OK";
    }

//    @Deprecated
//    @RequestMapping(value = "/run", method = RequestMethod.POST)
//    public String executeCommand(@RequestBody String code, @Autowired HttpServletRequest request) throws IOException {
//        UserSession userSession = getUserSession(request);
//        new Thread(() -> {
//            try {
//                Object result = userSession.getEmbeddedExecutor().eval(code);
//                userSession.sendResult(result == null ? null : result.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }).start();
//        return "OK";
//    }

    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public String getTables(@Autowired HttpServletRequest request) throws IOException {
        System.out.println("GET /tables");
        UserSession userSession = getUserSession(request);
        System.out.println("GET UserSession");
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
        for (Tuple2<String, String> schema : schemas) {
            JSONObject obj = new JSONObject();
            obj.put("tableName", schema._1());
            obj.put("schema", new JSONObject(schema._2()));
            result.put(obj);
        }
        return result.toString();
    }
}
