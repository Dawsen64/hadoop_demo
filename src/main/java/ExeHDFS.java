import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


/**
 * @Author: zqiusen@qq.com
 * @Date: 2022/3/20 18:19
 */
public class ExeHDFS {
    String hdfsPath = "/";
    String hdfsUrl = "hdfs://119.3.219.46";
    String urlAndPort = "hdfs://119.3.219.46:8020";

//    public static void main(String[] args)throws Exception{
//        //动态方式，获取abc.txt的绝对路径
//        String path = ExeHDFS.class.getClassLoader().getResource("upLoad.txt").getPath();
//        InputStream input = new FileInputStream(path) ;
//        System.out.println(path);
//    }

    public static void main(String[] args) {
        ExeHDFS testHDFS = new ExeHDFS();
        try {
            testHDFS.testView();
            testHDFS.testUpload();
            testHDFS.testCreate();
            testHDFS.testDownLoad();
            testHDFS.testView();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //查看HDFS文件
    public void testView() throws IOException, URISyntaxException, InterruptedException {
        System.out.println("==============================================================");
        System.out.println("View file:");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", urlAndPort);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        //加载配置文件
        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), conf, "root");

        Path path = new Path(hdfsPath);
        //查看当前path路径下的文件状态
        FileStatus[] list = hdfs.listStatus(path);
        if (list.length == 0) {
            System.out.println("HDFS is empty.");
        } else {
            for (FileStatus f : list) {
                System.out.printf("name: %s， folder: %s，size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
            }
        }
    }

    //.上传本地文件到HDFS
    public void testUpload() throws IOException, URISyntaxException, InterruptedException {
        System.out.println("==============================================================");
        System.out.println("UpLoad file:");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", urlAndPort);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //加载配置文件
        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), conf, "root");
        //获取本地文件的一个输入流,就是把upload.txt文件上传
//        InputStream in = new FileInputStream("src/main/resources/upload.txt");
        InputStream in = getClass().getClassLoader().getResourceAsStream("upload.txt");
        //获取一个输出流,输出到hadoop服务器
        OutputStream out = hdfs.create(new Path(hdfsPath + "upLoad_2019211565.txt"));
        IOUtils.copyBytes(in, out, conf);
        System.out.println("UpLoad successfully!");

    }

    //创建HDFS文件
    public void testCreate() throws Exception {
        System.out.println("==============================================================");
        System.out.println("Write file:");
        Configuration conf = new Configuration();

        conf.set("dfs.client.use.datanode.hostname", "true");

        conf.set("fs.defaultFS", urlAndPort);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //待写入文件内容
        //写入自己的姓名与学号信息
        byte[] buff = "Hello world! My name is zqs, my student id is 2019211565.".getBytes();
        //FileSystem为HDFS的API,通过此调用HDFS
        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), conf, "root");
        //文件目标路径，应填写hdfs文件路径
        Path dst = new Path(hdfsPath + "zqs_2019211565.txt");
        FSDataOutputStream outputStream = null;
        try {
            //写入文件
            outputStream = hdfs.create(dst);
            outputStream.write(buff, 0, buff.length);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                outputStream.close();
                //检查文件写入情况
                FileStatus files[] = hdfs.listStatus(dst);
                for (FileStatus file : files) {
                    //打印写入文件路径及名称
                    System.out.println(file.getPath());

                }
            }
        }
    }

    //从HDFS下载文件到本地
    public void testDownLoad() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("==============================================================");
        System.out.println(" Download file:");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", urlAndPort);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), conf, "root");
        //加载要下载的文件
        InputStream in = hdfs.open(new Path(hdfsPath + "zqs_2019211565.txt"));
        //下载文件
        OutputStream out = new FileOutputStream("/downLoad.2019211565.txt");
//        OutputStream out = getClass().getClassLoader()("downLoad.2019211565.txt");
        IOUtils.copyBytes(in, out, conf);
        System.out.println("DownLoad successfully!");
    }


}
