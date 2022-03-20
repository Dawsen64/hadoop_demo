import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @Author: zqiusen@qq.com
 * @Date: 2022/3/20 18:19
 */
public class ExeHDFS {
    //查看HDFS文件
    public void testView() throws IOException, URISyntaxException, InterruptedException {
        System.out.println("View file:");
        Configuration conf = new Configuration();
        conf . set(" dfs. client . use . datanode . hostname", "true ");
        conf . set("fs . defaultFS", "hdfs://121.36. 99.86:8020");
        FileSystem ndfs = FileSystem. get(new URI( "hdfs://121.36. 99.86"), conf, "root");
        Path path = new Path(hdfsPath);
        FiLeStatus[] list = hdfs.listStatus(path);
        if(list.length==0){
            System. out . printLn("HDFS is empty.");
        }else {
            for (FiLeStatus f : list) {
                System. out . printf("name: %s， folder: %s，size: %d\n"， f. getPath(), f. isDirectory(), f.getLen());
            }

        }