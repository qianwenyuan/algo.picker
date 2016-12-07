package fdu.bean.executor;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by slade on 2016/11/28.
 */


public class ShellExecutor {
    private Process p;
    private Reader reader;
    private Writer writer;
    private Reader error;

    class Reader extends Thread {
        private BufferedReader reader;

        Reader(InputStream inputStream) {
            this.reader = new BufferedReader(new InputStreamReader(inputStream));
        }

        public void run() {
            String line;
            try {
                while (!isInterrupted() && (line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class Writer extends Thread {
        private PrintWriter writer;
        private BlockingQueue<String> queue;

        public Writer(OutputStream outputStream) {
            writer = new PrintWriter(new OutputStreamWriter(outputStream));
            queue = new LinkedBlockingQueue<>(1);
        }

        public void write(String s) throws InterruptedException {
            queue.put(s);
        }

        public void run() {
            while (!isInterrupted()) {
                try {
                    String s = queue.take();
                    writer.println(s);
                    writer.flush();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void init() throws IOException {
        String shellPath = System.getProperty("taql.spark.home") + "/bin/spark-shell --master spark://ubuntu1:7077 --jars /home/scidb/taql-hive/spark-interactive-explore.jar,/home/scidb/taql-hive/spark-taql-0.0.2.jar --conf spark.sql.catalogImplementation=hive";
        System.out.println(shellPath);
        p = Runtime.getRuntime().exec(shellPath);
        reader = new Reader(p.getInputStream());
        writer = new Writer(p.getOutputStream());
        error = new Reader(p.getErrorStream());

        reader.start();
        writer.start();
        error.start();
    }

    public void destroy() {
        reader.interrupt();
        writer.interrupt();
        error.interrupt();
        p.destroy();
    }

    public String executeCommand(String command) {
        try {
            writer.write(command);
        } catch (InterruptedException e) {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            return writer.toString();
        }
        return "submitted";
    }
}
