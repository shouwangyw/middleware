package com.yw.middleware.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.csource.fastdfs.*;

import java.net.URLDecoder;

/**
 * @author yangwei
 */
@Slf4j
public class MasterSlaveFile {
    private static TrackerClient trackerClient = null;
    private static TrackerServer trackerServer = null;
    private static StorageServer storageServer = null;
    private static StorageClient1 client = null;
    // fdfs client的配置文件的路径
    private static String TRACKER_CLIENT_CONF = "/fdfs_client.conf";

    static {
        try {
            // 配置文件必须指定全路径
            String confName = MasterSlaveFile.class.getResource(TRACKER_CLIENT_CONF).getPath();
            // 配置文件全路径中如果有中文，需要进行utf8转码
            confName = URLDecoder.decode(confName, "utf-8");
            ClientGlobal.init(confName);
            trackerClient = new TrackerClient();
            trackerServer = trackerClient.getConnection();
            storageServer = null;
            client = new StorageClient1(trackerServer, storageServer);
        } catch (Exception e) {
            log.error("FastDFSClient error", e);
        }
    }

    public static String uploadFile(String filePath) throws Exception {
        String fileId = "";
        String fileExtName = "";
        if (filePath.contains(".")) {
            fileExtName = filePath.substring(filePath.lastIndexOf(".") + 1);
        } else {
            log.warn("上传失败，无效扩展名");
            return fileId;
        }
        try {
            fileId = client.upload_file1(filePath, fileExtName, null);
        } catch (Exception e) {
            log.error("Upload file \"" + filePath + "\" failed");
        } finally {
            trackerServer.close();
        }
        return fileId;
    }

    public static String uploadSlaveFile(String masterFileId, String prefixName, String slaveFilePath) throws Exception {
        String slaveFileId = "";
        String slaveFileExtName = "";
        if (slaveFilePath.contains(".")) {
            slaveFileExtName = slaveFilePath.substring(slaveFilePath.lastIndexOf(".") + 1);
        } else {
            log.warn("上传失败，无效扩展名");
            return slaveFileId;
        }
        try {
            slaveFileId = client.upload_file1(masterFileId, prefixName, slaveFilePath, slaveFileExtName, null);
        } catch (Exception e) {
            log.error("Upload file \"" + slaveFilePath + "\" failed");
        } finally {
            trackerServer.close();
        }
        return slaveFileId;
    }

    public static int downloadFile(String fileId, String localFile) throws Exception {
        int result = 0;
        try {
            result = client.download_file1(fileId, localFile);
        } catch (Exception e) {
            log.error("Download file \"" + localFile + "\" failed");
        } finally {
            trackerServer.close();
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        String masterFileId = uploadFile("/Users/yangwei/Desktop/picture/1.jpg");
        System.out.println("master file : " + masterFileId);
        // 下载上传成功的主文件
        downloadFile(masterFileId, "/Users/yangwei/Desktop/picture/master.jpg");

        // 第三个参数:待上传的从文件(由此可知，必须先本地生成从文件，再上传)
        String slaveFileId = uploadSlaveFile(masterFileId, "_120x120", "/Users/yangwei/Desktop/picture/2.jpg");
        System.out.println("slave file : " + masterFileId);
        // 下载上传成功的从文件
        downloadFile(slaveFileId, "/Users/yangwei/Desktop/picture/slave.jpg");
    }
}
