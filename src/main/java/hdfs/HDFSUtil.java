package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * @description HDFS操作类
 */
public class HDFSUtil {

    // 文件路径集合
    private Set<String> filPathStrSet = new HashSet<String>();

    /**
     *
     * @param uriStr HDFS URI
     * @param user 用户名
     * @return
     * @description 根据配置文件获取HDFS操作对象(客户端)
     */
    public FileSystem getHadoopFileSystem(String uriStr, String user) {
        FileSystem hdfs = null;

        Configuration configuration = new Configuration();
        try {
            hdfs = FileSystem.get(new URI(uriStr), configuration, user);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hdfs;
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param dirPath 文件夹路径
     * @return
     * @description 递归创建文件夹
     */
    public boolean createDir(FileSystem hdfs, String dirPath) {
        boolean result = false;

        Path path = new Path(dirPath);
        try {
            result = hdfs.mkdirs(path);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param pathStr 文件或文件夹路径
     * @return
     * @description 删除的是给定路径的最后一个文件或者文件夹
     */
    public boolean deletePath(FileSystem hdfs, String pathStr) {
        boolean result = false;

        Path path = new Path(pathStr);
        try {
            // API : delete(Path p, boolean recursive)
            result = hdfs.delete(path, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param oldPathStr 旧路径
     * @param newPathStr 新路径
     * @return
     * @description 重命名文件夹(测试未生效)
     */
    @Deprecated
    public boolean renamePath(FileSystem hdfs, String oldPathStr, String newPathStr) {
        boolean result = false;

        Path oldPath = new Path(oldPathStr);
        Path newPath = new Path(newPathStr);
        try {
            result = hdfs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param localFilePath 本地文件路径
     * @param hdfsFilePath HDFS文件路径
     * @description 上传文件(如果上传的路径不存在会自动创建；如果存在同名的文件，会覆盖)
     */
    public void uploadFile(FileSystem hdfs, String localFilePath, String hdfsFilePath) {
        Path localPath = new Path(localFilePath);
        Path hdfsPath = new Path(hdfsFilePath);
        try {
            hdfs.copyFromLocalFile(localPath, hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param hdfsFilePath HDFS文件路径
     * @param localFilePath 本地文件或文件夹路径
     * @descriiption 下载文件
     */
    public void downloadFile(FileSystem hdfs, String hdfsFilePath, String localFilePath) {
        Path hdfsPath = new Path(hdfsFilePath);
        Path localPath = new Path(localFilePath);
        try {
            // 1:false参数表示不删除源文件，4:true参数表示使用本地原文件系统，因为这个Demo程序是在Windows系统下运行的。
            hdfs.copyToLocalFile(false, hdfsPath ,localPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param fromHDFSFilePath HDFS源路径
     * @param toHDFSFilePath HDFS复制路径(不存在则自动创建)
     * @description HDFS之间文件的复制
     */
    public void copyFileBetweenHDFS(FileSystem hdfs, String fromHDFSFilePath, String toHDFSFilePath) {
        Path fromHDFSPath = new Path(fromHDFSFilePath);
        Path toHDFSPath = new Path(toHDFSFilePath);

        FSDataInputStream fsDataInputStream = null;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataInputStream = hdfs.open(fromHDFSPath);
            fsDataOutputStream = hdfs.create(toHDFSPath);

            // 使用缓冲区来加速读取文件，但是只要指定缓冲区大小即可，不必单独设置一个新的数组来接受
            // 最后一个布尔值表示是否使用完后关闭读写流。通常是false，如果不手动关会报错的
            IOUtils.copyBytes(fsDataInputStream, fsDataOutputStream, 1024*1024*64, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fsDataInputStream.close();
                fsDataOutputStream.close();
                hdfs.close();;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param pathStr 文件或文件夹路径
     * @description 文件的简单判断(包括是否存在，是否文件夹，还是文件)
     */
    public void checkPath(FileSystem hdfs, String pathStr) {
        boolean isExists = false;
        boolean isDirectory = false;
        boolean isFile = false;

        Path path = new Path(pathStr);
        try {
            isExists = hdfs.exists(path);
            isDirectory = hdfs.isDirectory(path);
            isFile = hdfs.isFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hdfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(!isExists) {
            System.out.println(pathStr + " doesn't exist. ");
        } else {
            System.out.print(pathStr + " exists. ");
            if(isDirectory) {
                System.out.println("And it's Directory");
            } else if(isFile) {
                System.out.println("And it's File");
            }
        }
    }

    /**
     *
     * @param hdfs HDFS客户端
     * @param pathStr 文件或文件夹路径
     * @return
     * @description 递归遍历文件夹(列出所有文件和空文件夹)
     *               hdfs.listStatus(path) : 若path为文件夹则获取下级文件(夹)信息，若path为文件则获取文件信息
     *               hdfs.listFiles(path, recursive) : false:获取下级文件目录中的文件 true:递归获取目录下的所有文件
     */
    public Set<String> fileRecursiveTraversal(FileSystem hdfs, String pathStr) {
        Path path = new Path(pathStr);

        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = hdfs.listStatus(path);
            if(fileStatuses.length == 0) {
                filPathStrSet.add(path.toUri().getPath());
            } else {
                for(FileStatus fileStatus : fileStatuses) {
                    if(fileStatus.isFile()) {
                        // getPath()方法获取的是FileStatus对象的URL路径。FileStatus.getPath().toUri().getPath()获取的路径才是不带url的路径
                        filPathStrSet.add(fileStatus.getPath().toUri().getPath());
                    } else {
                        fileRecursiveTraversal(hdfs, fileStatus.getPath().toUri().getPath());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return filPathStrSet;
    }

    /**
     *
     * @param args
     * @description 测试
     */
    public static void main(String[] args) {
        HDFSUtil hadoopTest = new HDFSUtil();
        // 单机模式（HDFS可以直接解析主机）
        FileSystem hdfs = hadoopTest.getHadoopFileSystem("hdfs://wonder1:9000", "wonder");
        // HA模式（HA模式的HDFS不能直接解析nameservice，需要引入HA相关配置，因此需要引入配置文件或者配置Configuration对象）
        FileSystem hdfsha = hadoopTest.getHadoopFileSystem("hdfs://ns1", "wonder");

//        hadoopTest.uploadFile(hdfsha, "C:/Users/13160/Desktop/文本格式.txt", "/file/文本格式.txt");

//        hadoopTest.createDir(hdfs, "/aaa/bbb/ccc");

//        hadoopTest.deletePath(hdfs, "/aaa/bbb/ccc");

//        hadoopTest.uploadFile(hdfs, "C:/Users/13160/Desktop/test.txt", "/aaa/bbb/ccc/test.txt");

//        hadoopTest.renamePath(hdfs, "/aaa/bbb/ccc", "aaa/bbb/ddd");

//        hadoopTest.downloadFile(hdfs, "/aaa/bbb/ccc/test.txt", "C:/Users/13160/Desktop/test.txt");

//        hadoopTest.copyFileBetweenHDFS(hdfs, "/aaa/bbb/ccc/test.txt", "/aaa/bbb/hello.txt");

//        hadoopTest.checkPath(hdfs, "/aaa/bbb/ccc/test.txt");

//        Set<String> set = hadoopTest.fileRecursiveTraversal(hdfs, "/");
//        for(String item : set) {
//            System.out.println(item);
//        }
    }
}
