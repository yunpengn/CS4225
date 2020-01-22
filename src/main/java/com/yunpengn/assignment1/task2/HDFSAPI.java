package com.yunpengn.assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;

public class HDFSAPI {
  Configuration conf = new Configuration();
  private FileSystem hdfs;

  public HDFSAPI(Path hdfsPath) throws IOException {
    hdfs = hdfsPath.getFileSystem(conf);
  }

  /**
   * Create directory
   */
  public void mkDir(Path path) throws IOException {
    hdfs.mkdirs(path);
  }

  /**
   * Upload file
   */
  public void copyLocalToHdfs(Path src, Path dst) throws IOException {
    hdfs.copyFromLocalFile(src, dst);
  }

  /**
   * delete file
   */
  @SuppressWarnings("deprecation")
  public void delFile(Path path) throws IOException {
    hdfs.delete(path);
  }

  /**
   * read file
   */
  public void readFile(Path path) throws IOException {
    //get file info
    FileStatus filestatus = hdfs.getFileStatus(path);

    FSDataInputStream in = hdfs.open(path);
    IOUtils.copyBytes(in, System.out, (int) filestatus.getLen(), false);
    System.out.println();
  }

  /**
   * get the time the file changed
   */
  public void getModifyTime(Path path) throws IOException {
    FileStatus[] files = hdfs.listStatus(path);
    for (FileStatus file : files) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
      String date = sdf.format(file.getModificationTime());
      System.out.println(file.getPath() + "\t" + date);
    }
  }

  /**
   * create file and write in HDFS
   */
  public void writeFile(Path path, String content) throws IOException {
    FSDataOutputStream os = hdfs.create(path);
    os.write(content.getBytes(StandardCharsets.UTF_8));
    os.close();
  }

  /**
   * list the file
   */
  public void listFiles(Path path) throws IOException {
    hdfs = path.getFileSystem(conf);
    FileStatus[] files = hdfs.listStatus(path);
    for (final FileStatus file : files) {
      if (!file.isDirectory()) {
        System.out.println("filename:" + file.getPath() + "\tsize:" + file.getLen());
      } else {
        Path newPath = new Path(file.getPath().toString());
        listFiles(newPath);
      }
    }
  }
}
