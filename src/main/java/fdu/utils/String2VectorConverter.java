package fdu.utils;

import fdu.algorithms.Vector;
import fdu.input.Converter;

/**
 * Created by sladezhang on 2016/10/8 0008.
 */
public class String2VectorConverter implements Converter<String, Vector> {
    @Override
    public Vector convert(String s) {
        String[] ss = s.split(" ");
        double[] d = new double[ss.length];
        for (int i = 0; i < d.length; i++) {
            d[i] = Double.parseDouble(ss[i]);
        }

        return Vector.valueOf(d);
    }
}
