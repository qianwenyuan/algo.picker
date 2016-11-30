package fdu.controller;

import fdu.bean.generator.ScalaDriverGenerator;
import fdu.service.OperationParserService;
import fdu.bean.executor.ShellExecutor;
import fdu.service.operation.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Created by slade on 2016/11/28.
 */
@RestController
public class generatorController {

    @Autowired
    private OperationParserService operationParserService;
    @Autowired
    private ShellExecutor shellExecutor;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public String generateDriver(@RequestBody String conf, ScalaDriverGenerator scalaDriverGenerator) throws IOException {
        Operation op = operationParserService.parse(conf);
        op.accept(scalaDriverGenerator);
        return shellExecutor.executeCommand(scalaDriverGenerator.generate());
    }
}
