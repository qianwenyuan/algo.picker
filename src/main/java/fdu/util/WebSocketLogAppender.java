package fdu.util;

import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.util.EnvUtil;
import ch.qos.logback.core.util.OptionHelper;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by guoli on 2017/5/9.
 */
public class WebSocketLogAppender<E> extends OutputStreamAppender<E> {

    private static OutputStream out = new MessageOutputStream(s -> {
        try {
            UserSessionPool.getInstance().broadcastLog(s); // TODO broadcast for now
        } catch (IOException e) {
            e.printStackTrace();
        }
    });

    protected boolean withJansi = false;

    private final static String WindowsAnsiOutputStream_CLASS_NAME = "org.fusesource.jansi.WindowsAnsiOutputStream";

    @Override
    public void start() {
        OutputStream targetStream = out;
        // enable jansi only on Windows and only if withJansi set to true
        if (EnvUtil.isWindows() && withJansi) {
            targetStream = getTargetStreamForWindows(targetStream);
        }
        setOutputStream(targetStream);
        super.start();
    }

    private OutputStream getTargetStreamForWindows(OutputStream targetStream) {
        try {
            addInfo("Enabling JANSI WindowsAnsiOutputStream for the console.");
            Object windowsAnsiOutputStream = OptionHelper.instantiateByClassNameAndParameter(WindowsAnsiOutputStream_CLASS_NAME, Object.class, context,
                    OutputStream.class, targetStream);
            return (OutputStream) windowsAnsiOutputStream;
        } catch (Exception e) {
            addWarn("Failed to create WindowsAnsiOutputStream. Falling back on the default stream.", e);
        }
        return targetStream;
    }

    /**
     * @return
     */
    public boolean isWithJansi() {
        return withJansi;
    }

    /**
     * If true, this appender will output to a stream which
     *
     * @param withJansi
     * @since 1.0.5
     */
    public void setWithJansi(boolean withJansi) {
        this.withJansi = withJansi;
    }

}
