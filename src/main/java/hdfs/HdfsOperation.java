package hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsOperation {
    public static void main(String[] args) throws IOException {
        Configuration conf = null;
        FileSystem fs = null;
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.234.136:8020");
        fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.listStatus(new Path("/user/hive/warehouse"));
        for(FileStatus fileStatus:listStatus){
            String fileName = fileStatus.getPath().getName();
            String type = fileStatus.isFile()?"file":"dir";
            long size = fileStatus.getLen();
            System.out.println(fileName+","+type+","+size);
        }
    }
}
