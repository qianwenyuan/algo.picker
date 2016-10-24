package fdu.algorithms;

import java.util.Arrays;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class Vector {
    private double[] elems;

    private Vector(double[] elems){
        this.elems = elems;
    }

    public static Vector valueOf(double[] vector){
        if (vector == null) throw new IllegalArgumentException("Param is null.");
        return new Vector(Arrays.copyOf(vector, vector.length));
    }

    public int size(){
        return elems.length;
    }

    public double get(int index){
        if(!withinBound(index)){
            throw new ArrayIndexOutOfBoundsException("index : " + index + ", length : " + elems.length);
        }

        return elems[index];
    }

    public double[] asArray(){
        return Arrays.copyOf(elems, elems.length);
    }

    @Override
    public String toString() {
        return "Vector{" +
                "elems=" + Arrays.toString(elems) +
                '}';
    }

    private boolean withinBound(int index) {
        return index >= 0 && index < elems.length;
    }
}
