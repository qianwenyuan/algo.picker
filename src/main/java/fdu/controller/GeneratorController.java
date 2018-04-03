package fdu.controller;

import fdu.bean.executor.ShellExecutor;
import fdu.bean.generator.ScalaDriverGenerator;
import fdu.service.OperationParserService;
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
public class GeneratorController {

    private final OperationParserService operationParserService;
    private final ShellExecutor shellExecutor;

    @Autowired
    public GeneratorController(OperationParserService operationParserService, ShellExecutor shellExecutor) {
        this.operationParserService = operationParserService;
        this.shellExecutor = shellExecutor;
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public String generateDriver(@RequestBody String conf, @Autowired ScalaDriverGenerator scalaDriverGenerator) throws IOException {
        Operation op = operationParserService.parse(conf).getRootOperation();
        op.accept(scalaDriverGenerator);
        String program = scalaDriverGenerator.generate();
        return shellExecutor.executeCommand(program);
    }
}
