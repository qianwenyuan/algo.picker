package fdu.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

/**
 * Created by guoli on 2017/5/10.
 */
public class MessageOutputStream extends OutputStream {

    private OutputStream backup;
    private UserEndPoint<String> f;

    MessageOutputStream(UserEndPoint<String> f) {
        this.f = f;
    }

    MessageOutputStream(OutputStream backup) {
        this.backup = backup;
    }

    public void setOutFunction(UserEndPoint<String> f) {
        this.f = f;
    }

    @Override
    public void write(int b) throws IOException {
        if (f == null) backup.write(b);
        else f.accept(Integer.toString(b));
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (f == null) backup.write(b);
        else f.accept(new String(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (f == null) backup.write(b, off, len);
        else {
            StringWriter s = new StringWriter();
            s.write(new String(b), off, len);
            f.accept(s.toString());
        }
    }
}
