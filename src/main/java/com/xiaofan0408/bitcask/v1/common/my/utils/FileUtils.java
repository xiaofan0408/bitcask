package com.xiaofan0408.bitcask.v1.common.my.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuzefan  2019/5/4 17:21
 */
public class FileUtils {

  public static List<String> getFile(String path) throws Exception{
      List<String> fileName = new ArrayList<>();
      File file = new File(path);
      if(!file.exists()) {
          file.mkdir();
      }
      File[] array = file.listFiles();
      if (array == null) {
          return fileName;
      }
      for(int i=0;i<array.length;i++) {
          if (array[i].isFile()) {
              fileName.add(array[i].getName());
          } else if (array[i].isDirectory()){
              List<String> childrenFileNames = getFile(array[i].getPath());
              fileName.addAll(childrenFileNames);
          }
      }
      return fileName;
  }

    public static List<File> listFile(String path) throws Exception{
        List<File> fileList = new ArrayList<>();
        File file = new File(path);
        if(!file.exists()) {
            file.mkdir();
        }
        File[] array = file.listFiles();
        if (array == null) {
            return new ArrayList<>();
        }
        for(int i=0;i<array.length;i++) {
            if (array[i].isFile()) {
                fileList.add(array[i]);
            } else if (array[i].isDirectory()){
                List<File> childrenFileNames = listFile(array[i].getPath());
                fileList.addAll(childrenFileNames);
            }
        }
        return fileList;
    }

}



