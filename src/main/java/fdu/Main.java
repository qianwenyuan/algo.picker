package fdu;

import java.io.IOException;

/**
 * Created by sladezhang on 2016/10/15 0015.
 */
public class Main {
    public static void main(String[] args) {

        System.out.println(returnMethod());
    }
    private static int returnMethod() {
        int a = 1;
        try{
            throw new IOException();
        }catch (IOException exception){
            return a;
        }finally {
            return 2;
        }
    }
}
