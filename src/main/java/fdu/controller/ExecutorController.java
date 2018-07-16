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

    public void write2log(String log_message) throws IOException{
        BufferedWriter writer = new BufferedWriter(new FileWriter("../his_status.json",true));
        writer.append(log_message);
        writer.close();
    }

    public Integer progress=0;
    public Integer status=0;
    String task_id = "";
    String job_id = "";
    String log_message="";
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
                            File file = new File("../his_status.txt");
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
                        } catch (IOException ex) {ex.printStackTrace();}
                        if (!if_job_exist) {
                            BufferedWriter writer = new BufferedWriter(new FileWriter("../his_status.txt", true));
                            writer.append(new_status + "\n");
                            writer.close();
                        }
                    } catch (FileNotFoundException e) {}
                    try {
                        BufferedWriter writer = new BufferedWriter(new FileWriter("../result_list.txt", true));
                        writer.append(job.getTable()+"\n");
                        writer.close();
                    } catch (Exception e) {}

                    task_id = job.getJid()+"_"+String.valueOf(System.currentTimeMillis());
                    //write2log(log_message);

                    //create_table_in_DFM(userSession, job.getJid(),job.getTable());


                    //createTable
//                    userSession.makePost(new URL("http://"+ Config.getDFMAddress()+":8080/project/create/"+task_id+"/"+job.getTable()), generateCreateContent(userSession, job.getTable()));

//                    userSession.makePost(new URL("http://"+Config.getDFMAddress()+":8080/project/"+task_id+"/build"), getBuildContent(), true);
                    //getstatus
//                    userSession.makeGet(new URL("http://"+Config.getDFMAddress()+":8080/job/"+task_id+"/status"));


                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "ok"));
                    progress=100;
                } catch (HiveTableNotFoundException e1) {
                    status = -1;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File("../his_status.txt");
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
                            BufferedWriter writer = new BufferedWriter(new FileWriter("../his_status.txt", true));
                            writer.append(new_status + "\n");
                            writer.close();
                        }
                    } catch (IOException e) {}

                    userSession.makeGet(new URL("http://" + Config.getAddress() + ":1880/jid/" + job.getJid() + "/status/" + "bbcz"));
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
                    status = -1;
                    String new_status = new String(job_id+","+status.toString());
                    try {
                        boolean if_job_exist = false;
                        try {
                            File file = new File("../his_status.txt");
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
                            BufferedWriter writer = new BufferedWriter(new FileWriter("../his_status.txt", true));
                            writer.append(new_status + "\n");
                            writer.close();
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
                            File file = new File("../his_status.txt");
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
                                BufferedWriter writer = new BufferedWriter(new FileWriter("../his_status.txt", true));
                                writer.append(new_status + "\n");
                                writer.close();
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
                File file = new File("../his_status.txt");
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
    public String gettable(@RequestBody String table_name, @Autowired HttpServletRequest request) throws IOException{
        UserSession userSession = getUserSession(request);
        return userSession.getEmbeddedExecutor().getTable(table_name);
    }

    @RequestMapping(value = "/resultlist", method = RequestMethod.GET)
    public String get_result_list(@Autowired HttpServletRequest request) throws IOException {
        StringBuilder jsonStr = new StringBuilder();
        String list = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader((new File("../result_list.txt"))));
            String temp = "";
            while ((temp = reader.readLine())!=null) {
                list = list+temp+"/";
            }
            reader.close();
        } catch (FileNotFoundException ex) {ex.printStackTrace();}
        return list;
    }
}
