package fdu.input;

/**
 * Created by sladezhang on 2016/10/3 0003.
 */
public interface Converter<K,V> {
    V convert(K k);
}
