package fdu.controller;

import fdu.Config;
import fdu.exceptions.HiveTableNotFoundException;
import fdu.service.Job;
import fdu.service.OperationParserService;
import fdu.service.operation.operators.CanProduce;
import fdu.util.ResultSerialization;
import fdu.util.UserSession;
import fdu.util.UserSessionPool;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

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


    public String getCreateContent(UserSession userSession, String tablename) {
        // TODO
        String dimensions[] = userSession.getEmbeddedExecutor().getTableColumns(tablename);
        String sum="";
        String measures="";
        String timeColumn="";
        String timeFormat="";
        return "dimensions="+String.join(",", dimensions)+"&"+ "sum="+sum+"&" +"measures="+measures+"&" +"timeColumn="+timeColumn+"&" +"timeFormat="+timeFormat;
    }

    public Map<String,String> generateCreateContent(UserSession userSession, String tablename){
        Map<String, String> result = new HashMap<>();
        String dimensions[] = userSession.getEmbeddedExecutor().getTableColumns(tablename);
        result.put("dimensions", String.join(",", dimensions));
        return result;
    }

    public String getBuildContent() {
        // TODO
        String startTime="";
        String endTime="";
        return "startTime="+startTime+"&" +"endTime="+endTime;
    }

    public Integer progress=0;
    public Integer status=0;
    String task_id = "";
    @RequestMapping(value = "/node", method = RequestMethod.POST)
    @ResponseBody
    public String generateDriver(@RequestBody final String conf, @Autowired HttpServletRequest request) throws IOException {
        final UserSession userSession = getUserSession(request);
        Config.setAddress(request.getRemoteAddr());
        new Thread(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                long end;
                Job job = operationParserService.parse(conf);
                try {
                try {
                    status = 1;
                    Object res;
                    if (userSession.getEmbeddedExecutor().tableExists(job.getTable())) {
                        res = userSession.getSparkSession().table(job.getTable());
                    } else {
                        res = ((CanProduce) job.getRootOperation()).executeCached(userSession);
                        if (res != null && res instanceof Dataset) {
                            ((Dataset) res).write().saveAsTable(job.getTable());
                        }
                    }
                    String resultString = ResultSerialization.toString(res);
                    userSession.sendResult(Config.getAddress(), job.getJid(), resultString);
                    System.out.println(resultString);
                    System.out.println("Job Finished");
                    end = System.currentTimeMillis();
                    System.out.print("\nRunning time: ");
                    System.out.println(end - start);
                    status = 2;
                    //create_table_in_DFM(userSession, job.getJid(),job.getTable());
                    task_id = job.getJid()+"_"+String.valueOf(System.currentTimeMillis());
                    //createTable
                    userSession.makePost(new URL("http://"+ Config.getDFMAddress()+":8080/project/create/"+task_id+"/"+job.getTable()), generateCreateContent(userSession, job.getTable()));

                    userSession.makePost(new URL("http://"+Config.getDFMAddress()+":8080/project/"+task_id+"/build"), getBuildContent(), true);
                    //getstatus
                    userSession.makeGet(new URL("http://"+Config.getDFMAddress()+":8080/job/"+task_id+"/status"));


                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "ok"));
                    progress=100;
                } catch (HiveTableNotFoundException e1) {
                        userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "bbcz"));
                } catch (Exception e) {
                    e.printStackTrace();
                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "error"));
                    status = -1;
                }
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        return "OK";
    }

    /*
    @RequestMapping(,headers = "")
    public String build(@Autowired HttpServletRequest request) throws  IOException {
        UserSession userSession = getUserSession(request);

        //buildTable
        userSession.makePost(new URL("http://"+Config.getDFMAddress()+":8080/project/"+task_id+"/build"), getBuildContent(), true);
        //getstatus
        userSession.makeGet(new URL("http://"+Config.getDFMAddress()+":8080/job/"+task_id+"/status"));

    }
    */

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    public String getTablelist(@Autowired HttpServletRequest request) throws IOException {
        System.out.println("GET /list");
        UserSession userSession = getUserSession(request);
        System.out.println("GET UserSession");
        //userSession.makePost(new URL("http://"+ Config.getDFMAddress()+":8080/project/create/"+"test_4_8_"+String.valueOf(System.currentTimeMillis())+"/"+"output_0024414100_20180408_1513"), generateCreateContent(userSession, "output_0024414100_20180408_1513"));
        return new JSONArray(userSession.getEmbeddedExecutor().getTableNames()).toString();
    }

    @RequestMapping(value = "/columns", method = RequestMethod.POST)
    public String getTable(@RequestBody String tablename, @Autowired HttpServletRequest request) throws IOException {
        UserSession userSession = getUserSession(request);
        Integer eql= tablename.indexOf("=");
        return new JSONArray(userSession.getEmbeddedExecutor().getTableColumns(tablename.substring(eql+1))).toString();
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

    @RequestMapping(value = "/statusquery", method = RequestMethod.POST)
    public String statusReport(@Autowired HttpServletRequest request) throws IOException{
        //UserSession userSession = getUserSession(request);
        return status.toString()+"/"+progress.toString()+"%";
    }
}
