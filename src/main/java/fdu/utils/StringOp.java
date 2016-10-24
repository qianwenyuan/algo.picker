package fdu.utils;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class StringOp {
    public static String bigCapital(String s){
        if (s == null) {
            throw new IllegalArgumentException("String is null.");
        }
        // already upper case.
        if (Character.isUpperCase(s.charAt(0))){
            return s;
        }
        char[] arr = s.toCharArray();
        arr[0] = Character.toUpperCase(arr[0]);
        return String.valueOf(arr);
    }
}
