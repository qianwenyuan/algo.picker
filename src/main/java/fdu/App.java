package fdu;

import fdu.algorithms.AlgoConfParser;

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
        AlgoConfParser algoConfParser = new AlgoConfParser("algo.conf");
        algoConfParser.parse();
        System.out.println(algoConfParser.getOperationTree().getOperation().toString());
    }
}
