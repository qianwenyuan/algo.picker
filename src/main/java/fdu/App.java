package fdu;

import fdu.operation.Generator.ScalaDriverGenerator;
import fdu.operation.Operation;

/**
 * Hello world!
 *
 */
public class App 
{

    public static void main(String[] args) throws Exception {
        OperationParser parser = new OperationParser("algo.conf");
        Operation op = parser.parse();
        ScalaDriverGenerator generator = new ScalaDriverGenerator();
        op.accept(generator);
        System.out.println(generator.generate());
    }
}
