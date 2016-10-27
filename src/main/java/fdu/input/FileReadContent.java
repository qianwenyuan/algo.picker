/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author Lu Chang
 */
public class FileReadContent {
    
    public static String fileReadContent(String path, String encoding) throws FileNotFoundException, IOException, UnsupportedEncodingException  {
        File file       = new File(path);
        long fileLength = file.length();
        
        FileInputStream in        = new FileInputStream(file);
        StringBuilder fileContent = new StringBuilder("");
                
        for (long length = Integer.MAX_VALUE; length < fileLength; length += Integer.MAX_VALUE) {
            byte[] filePatchContent = new byte[Integer.MAX_VALUE];
            in.read(filePatchContent);
            fileContent.append(new String(filePatchContent, encoding));
        }
        
        byte[] fileLastPatchContent = new byte[(int) fileLength % Integer.MAX_VALUE];
        in.read(fileLastPatchContent);
        in.close();
        fileContent.append(new String(fileLastPatchContent, encoding));
        
        return fileContent.toString();
    }
    
    public static String fileReadContent(String path) throws FileNotFoundException, IOException, UnsupportedEncodingException {
        return fileReadContent(path, "UTF-8");
    }
    
}
