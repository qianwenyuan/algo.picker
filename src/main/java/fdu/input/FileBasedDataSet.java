package fdu.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by sladezhang on 2016/10/1 0001.
 */
public class FileBasedDataSet<E> implements DataSet<E> {
    private File file;
    private Converter<String, E> converter;

    public FileBasedDataSet(File file, Converter<String, E> converter){
        if (file == null){
           throw new IllegalArgumentException("File is null.");
        }
        if (converter == null){
            throw new IllegalArgumentException("Converter is null.");
        }

        if(!file.isFile()){
            throw new IllegalArgumentException("File " + file.toString() + "is invalid.");
        }
        this.file = file;
        this.converter = converter;
    }

    @Override
    public Iterator<E> iterator() {
        return new FileLineIterator<>(file, converter);
    }

    private class FileLineIterator<E> implements Iterator<E>{
        private BufferedReader reader;
        private String bufferedLine;
        private boolean isFinished;
        private Converter<String, E> converter;
        public FileLineIterator(File file, Converter<String, E> converter){
            try {
                reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
                this.converter = converter;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean hasNext() {
            if(isFinished) return false;
            if(bufferedLine != null) return true;
            try {
                bufferedLine = reader.readLine();
                if(bufferedLine == null){
                    isFinished = true;
                    return false;
                }
                return true;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more line.");
            }
            String curLine = bufferedLine;
            bufferedLine = null;
            return converter.convert(curLine);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported.");
        }
    }
}
