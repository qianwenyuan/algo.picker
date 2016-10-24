package fdu;

import fdu.runner.AlgoRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{

    public static void main(String[] args) throws Exception {
        File confFile = new File("algo.conf");
        List<String> lines;
        try {
            lines = Files.readAllLines(confFile.toPath(), Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        AlgoRunner runner = new AlgoRunner();
        for (String conf : lines){
            runner.run(conf);
        }
    }
}
