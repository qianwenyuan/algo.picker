package fdu;

import fdu.bean.executor.ShellExecutor;
import fdu.bean.generator.ScalaDriverGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;

/**
 * Hello world!
 */
@SpringBootApplication
public class App {
    @Bean
    @Scope("request")
    public ScalaDriverGenerator scalaDriverGenerator() {
        return new ScalaDriverGenerator();
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    @Scope(value = "session", proxyMode = ScopedProxyMode.TARGET_CLASS)
    public ShellExecutor shellExecutor(){
        return new ShellExecutor();
    }

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
