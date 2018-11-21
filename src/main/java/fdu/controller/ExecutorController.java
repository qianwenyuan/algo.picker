package fdu.controller;

import fdu.Config;
import fdu.exceptions.HiveTableNotFoundException;
import fdu.service.Job;
import fdu.service.OperationParserService;
import fdu.service.operation.operators.CanProduce;
import fdu.util.ResultSerialization;
import fdu.util.UserSession;
import fdu.util.UserSessionPool;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;

import javax.annotation.processing.FilerException;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;

/**
 * Created by liangchg on 2017/4/6.
 */
@RestController
public class ExecutorController {

    private final OperationParserService operationParserService;

    public static String ip=Config.DEFAULT_FUCK_ADDRESS;
    public static String port=Config.DEFAULT_FUCK_PORT;

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

    public void write2log(String path, String log_message) throws IOException{
        Files.write(Paths.get(path), new String(log_message+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
//
//        BufferedWriter writer = new BufferedWriter(new FileWriter("~/his_status.txt",true));
//        writer.append(log_message);
//        writer.close();
    }

    public JSONArray trans(JSONArray jsarr) {
        JSONObject jobj1 = (JSONObject) jsarr.getJSONObject(0).get("schema");

        JSONArray hive_table_meta = new JSONArray();
        JSONArray jobj2 = (JSONArray) jobj1.get("fields");
        for (Object obj:jobj2) {
            JSONObject jobj = (JSONObject) obj;
            JSONObject tempobj = new JSONObject();
            boolean flag = true;
            if (jobj.has("name")&&jobj.has("type")) {
                tempobj.put("name",jobj.get("name"));
                tempobj.put("type", jobj.get("type"));
                hive_table_meta.put(tempobj);
            }
        }
        return hive_table_meta;
    }

    public String createContent(Job job, UserSession userSession, String tag) {
        String id = job.getJid();
        String filename = job.getTable();
        JSONObject jo = new JSONObject();
        jo.put("tablename",job.getTable());
        jo.put("splitname",",");
        JSONArray hive_table_list = new JSONArray();

        JSONArray array = new JSONArray("["+filename+"]");
        List<String> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            list.add(array.getString(i));
        }
        String[] tables = list.toArray(new String[list.size()]);
        Tuple2<String, String>[] schemas = userSession.getEmbeddedExecutor().getTableSchemas(tables);
        JSONArray ans = new JSONArray();
        for (Tuple2<String, String> schema : schemas) {
            JSONObject obj = new JSONObject();
            obj.put("tableName", schema._1());
            obj.put("schema", new JSONObject(schema._2()));
            ans.put(obj);
        }
        System.out.println(tables[0]);
        System.out.println(ans.toString());
        ans = trans(ans);


        JSONObject jobj = new JSONObject();
        jobj.put("tablename",filename);
        jobj.put("splitname",",");
        jobj.put("hive_table_meta",ans);

        hive_table_list = new JSONArray("["+jobj+"]");
        JSONObject joo = new JSONObject();
        joo.put("id",id);
        joo.put("filename",filename);
        joo.put("hive_table_list",hive_table_list);
        joo.put("tag",tag);

        String result = joo.toString();

        System.out.println(result);

        return result;
    }

    public Integer progress=0;
    public Integer status=0;
    String task_id = "";
    String job_id = "";
    String log_message="";
    String resultPath = "result_list.txt";
    String hisPath = "his_status.txt";
    @RequestMapping(value = "/node", method = RequestMethod.POST)
    @ResponseBody
    public String generateDriver(@RequestBody final String conf, @Autowired HttpServletRequest request) throws IOException {
        //write2log(new String(String.valueOf(System.currentTimeMillis())+"-- Json: "+conf+"\n"));

        final UserSession userSession = getUserSession(request);
        Config.setAddress(request.getRemoteAddr());
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("\n"+conf+"\n");
                long start = System.currentTimeMillis();
                long end;
                Job job = operationParserService.parse(conf);

                try {
                try {
                    status = 1;
                    job_id = job.getJid();
                    //System.out.print("\njid="+job_id+","+"table="+job.getTable()+"\n");
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

                    /// !!!!!!!!!


                    userSession.makePost(new URL("http://"+ip+":"+port+"/dmp-api/external/dataUseJobResult"),
                            createContent(job, userSession, "true"), true);

                    /// !!!!!!!!!
                    /*
                    try {
                        out.write(new String(String.valueOf(System.currentTimeMillis())+"-- Job Finished. Running time: "+
                                String.valueOf(end-start)+"\n").getBytes());
                    } catch (FilerException fe) {
                        fe.printStackTrace();
                    }
                    */
                    status = 2;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File(hisPath);
                            if (file.exists() && file.isFile()) {
                                BufferedReader reader = new BufferedReader(new FileReader((file)));
                                String temp = "";
                                while ((temp = reader.readLine()) != null) {
                                    String[] kvpairtemp = temp.split(",");
                                    if (kvpairtemp[0].equals(job_id)) {
                                        reader.close();
                                        if_job_exist = true;
                                    }
                                }
                                reader.close();
                            }
                        } catch (IOException ex) {ex.printStackTrace();}
                        if (!if_job_exist) {
//                            FileWriter writer = new FileWriter("~/his_status.txt", true);
//                            writer.append(new_status + "\n");
//                            writer.close();
                            write2log(hisPath, new_status);
//                            Files.write(Paths.get("~/his_status.txt"), new_status.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }
                    } catch (FileNotFoundException e) {}
                    System.out.println("Start writing result!");
                    try {
                        write2log(resultPath, job.getTable());
//                        BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath, true));
//                        System.out.println("Write result success!");
//                        writer.append(job.getTable()+"\n");
//                        writer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    task_id = job.getJid()+"_"+String.valueOf(System.currentTimeMillis());
                    //write2log(log_message);

                    //create_table_in_DFM(userSession, job.getJid(),job.getTable());


                    //createTable
//                    userSession.makePost(new URL("http://"+ Config.getDFMAddress()+":8080/project/create/"+task_id+"/"+job.getTable()), generateCreateContent(userSession, job.getTable()));

//                    userSession.makePost(new URL("http://"+Config.getDFMAddress()+":8080/project/"+task_id+"/build"), getBuildContent(), true);
                    //getstatus
//                    userSession.makeGet(new URL("http://"+Config.getDFMAddress()+":8080/job/"+task_id+"/status"));


                    //userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "ok"));
                    progress=100;
                } catch (HiveTableNotFoundException e1) {
                    e1.printStackTrace();
                    userSession.makePost(new URL("http://"+ip+":"+port+"/dmp-api/external/dataUseJobResult"),
                            createContent(job, userSession, "false"), true);
                    status = -1;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File(hisPath);
                            if (file.isFile() && file.exists()) {
                                BufferedReader reader = new BufferedReader(new FileReader((file)));
                                String temp = "";
                                while ((temp = reader.readLine()) != null) {
                                    String[] kvpairtemp = temp.split(",");
                                    if (kvpairtemp[0].equals(job_id)) {
                                        reader.close();
                                        if_job_exist = true;
                                    }
                                }
                                reader.close();
                            }
                        } catch (FileNotFoundException ex) {ex.printStackTrace();}
                        if (!if_job_exist) {
                            write2log(hisPath, new_status);
//                            BufferedWriter writer = new BufferedWriter(new FileWriter("~/his_status.txt", true));
//                            writer.append(new_status + "\n");
//                            writer.close();
                        }
                    } catch (IOException e) {}

                    //userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "bbcz"));
                    /*
                    try {
                        out.write(new String(String.valueOf(System.currentTimeMillis())+"-- ERROR: 表不存在\n").getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    */
                    log_message = new String(String.valueOf(System.currentTimeMillis())+"-- ERROR: 表不存在\n");
                    //write2log(log_message);
                } catch (Exception e) {
                    e.printStackTrace();
                    userSession.makePost(new URL("http://"+ip+":"+port+"/dmp-api/external/dataUseJobResult"),
                            createContent(job, userSession, "false"), false);
                    status = -1;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File(hisPath);
                            if (file.isFile() && file.exists()) {
                                BufferedReader reader = new BufferedReader(new FileReader((file)));
                                String temp = "";
                                while ((temp = reader.readLine()) != null) {
                                    String[] kvpairtemp = temp.split(",");
                                    if (kvpairtemp[0].equals(job_id)) {
                                        reader.close();
                                        if_job_exist = true;
                                    }
                                }
                                reader.close();
                            }
                        } catch (FileNotFoundException ex) {ex.printStackTrace();}
                        if (!if_job_exist) {
                            write2log(hisPath,new_status);
//                            BufferedWriter writer = new BufferedWriter(new FileWriter("~/his_status.txt", true));
//                            writer.append(new_status + "\n");
//                            writer.close();
                        }
                    } catch (IOException e1) {}

                    e.printStackTrace();
                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "error"));
                    /*
                    try {
                        out.write(new String(String.valueOf(System.currentTimeMillis())+"-- ERROR: Runtime Error\n").getBytes());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    */
                    log_message = new String(String.valueOf(System.currentTimeMillis())+"-- ERROR: Runtime Error\n");

                }
                } catch (MalformedURLException e) {

                    status = -1;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File(hisPath);
                            if (file.isFile() && file.exists()) {
                                BufferedReader reader = new BufferedReader(new FileReader((file)));
                                String temp = "";
                                while ((temp = reader.readLine()) != null) {
                                    String[] kvpairtemp = temp.split(",");
                                    if (kvpairtemp[0].equals(job_id)) {
                                        reader.close();
                                        if_job_exist = true;
                                    }
                                }
                                reader.close();
                            }
                        } catch (FileNotFoundException ex) {ex.printStackTrace();}
                        if (!if_job_exist) {
                            try {
                                write2log(hisPath,new_status);
//                                BufferedWriter writer = new BufferedWriter(new FileWriter("~/his_status.txt", true));
//                                writer.append(new_status + "\n");
//                                writer.close();
                            } catch (Exception e2) {e2.printStackTrace();}
                        }
                    } catch (Exception e1) {}

                    e.printStackTrace();
                }
            }
        }).start();
        //write2log(log_message);
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
    public String statusReport(@RequestBody String jid, @Autowired HttpServletRequest request) throws IOException{
        //UserSession userSession = getUserSession(request);
        if (jid == job_id) {
            return status.toString();
        }
        else {
            try {
                File file = new File(hisPath);
                if (file.isFile() && file.exists()) {
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    String temp = "";
                    while ((temp = reader.readLine()) != null) {
                        String[] kvpairtemp = temp.split(",");
                        if (kvpairtemp[0].equals(jid)) {
                            reader.close();
                            return kvpairtemp[1];
                        }
                    }
                    reader.close();
                }
            } catch (FileNotFoundException ex) {ex.printStackTrace();}
            return "-2";
        }
    }

    @RequestMapping(value = "/table", method = RequestMethod.POST)
    public String gettable(@RequestBody String table_name, @Autowired HttpServletRequest request) throws IOException, SQLException {
//        UserSession userSession = getUserSession(request);
//        return userSession.getEmbeddedExecutor().getTable(table_name);
//        System.out.println(gettable(table_name,request));

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10010/default;AuthMech=0;transportMode=binary");
        Statement stmt = con.createStatement();
        JSONObject ans = new JSONObject();


        String sql = "desc "+table_name;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        JSONArray columns = new JSONArray();
        res.next();
        columns.put(res.getString(1));
        while (res.next()){
            columns.put(res.getString(1));
        }


//        get rows
        sql = "select * from "+table_name+" limit 20000";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);

        res.next();
        JSONArray data=new JSONArray();
//        ResultSetMetaData rsmd = res.getMetaData();
        Integer columncount = res.getMetaData().getColumnCount();
        JSONArray row=new JSONArray();
        Double prob = 0.0;
        for (int i=1;i<=columncount;++i) {
            row.put(res.getString(i));
//            if (res.getString(i).contains("values"))
//                System.out.println(i);
        }
        if (table_name.contains("lr_predict")) {
            columns.put("prob");
            JSONObject jobj = new JSONObject(res.getString(columncount-1));
//            System.out.println("\n"+jobj.toString()+"\n");
            JSONArray jarr = jobj.getJSONArray("values");
            prob = (Double)jarr.getDouble(1);
            row.put(prob);
        }

//        row+=res.getString(columncount);
        data.put(row);
        Integer count=1;
        while (res.next()) {
            row=new JSONArray();
//            rsmd = res.getMetaData();
            for (int i=1;i<=columncount;++i) {
                row.put(res.getString(i));
            }
            if (table_name.contains("lr_predict")) {
                JSONObject jobj = new JSONObject(res.getString(columncount-1));
//                System.out.println("\n"+jobj.toString()+"\n");
                JSONArray jarr = jobj.getJSONArray("values");
                prob = (Double)jarr.getDouble(1);
                row.put(prob);
            }
//            row+=res.getString(columncount)+"]";
            data.put(row);
            count++;
        }
//        System.out.println(data);
//        String scount = count.toString();
        ans.put("columns",columns);
        ans.put("data",data);
        ans.put("count",count);

//        System.out.println(ans.toString());

        con.close();
        return ans.toString();
    }

    @RequestMapping(value = "/resultlist", method = RequestMethod.GET)
    public String get_result_list(@Autowired HttpServletRequest request) throws IOException {
        StringBuilder jsonStr = new StringBuilder();
        String list = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader((new File(resultPath))));
            String temp = "";
            while ((temp = reader.readLine())!=null) {
                list = list+temp+"/";
            }
            reader.close();
        } catch (FileNotFoundException ex) {ex.printStackTrace();}
        return list;
    }
}
