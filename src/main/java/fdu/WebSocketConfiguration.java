package fdu;

import fdu.service.LogWebSocketHandler;
import fdu.service.ReplWebSocketHandler;
import fdu.service.ResultWebSocketHandler;
import fdu.util.WsHandShakeInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Created by guoli on 2017/5/10.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfiguration implements WebSocketConfigurer {

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry// .addHandler(replWebSocketHandler(), "/repl")
                .addHandler(resultWebSocketHandler(), "/result")
                .addHandler(logWebSocketHandler(), "/log")
                .setAllowedOrigins("*")
                .addInterceptors(new WsHandShakeInterceptor());
    }

    @Bean
    public ReplWebSocketHandler replWebSocketHandler() {
        return new ReplWebSocketHandler();
    }

    @Bean
    public ResultWebSocketHandler resultWebSocketHandler() {
        return new ResultWebSocketHandler();
    }

    @Bean
    public LogWebSocketHandler logWebSocketHandler() {
        return new LogWebSocketHandler();
    }

}
