package fdu.controller;

import fdu.service.OperationParserService;
import fdu.service.operation.CanProduce;
import fdu.service.operation.Operation;
import fdu.util.UserSession;
import fdu.util.UserSessionPool;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

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

    private UserSession getUserSession(HttpServletRequest request) {
       return UserSessionPool.getInstance().addOrGetUserSession(request.getSession().getId());
    }

    @RequestMapping(value = "/node", method = RequestMethod.POST)
    public String generateDriver(@RequestBody String conf, @Autowired HttpServletRequest request) {
        UserSession userSession = getUserSession(request);
        new Thread(() -> {
            Operation op = operationParserService.parse(conf);
            // TODO
            Dataset<Row> res = ((CanProduce<Dataset<Row>>)op).executeCached(userSession);
            res.show();
        }).start();
        return "OK";
    }

    @RequestMapping(value = "/run", method = RequestMethod.POST)
    public String executeCommand(@RequestBody String code, @Autowired HttpServletRequest request) {
        UserSession userSession = getUserSession(request);
        new Thread(() -> {
            try {
                Object result = userSession.getEmbeddedExecutor().eval(code);
                userSession.sendResult(result == null ? null : result.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        return "OK";
    }
}