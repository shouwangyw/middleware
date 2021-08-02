package com.yw.middleware.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import java.net.URLDecoder;

import static com.sun.xml.internal.ws.commons.xmlutil.Converter.UTF_8;

/**
 * @author yangwei
 */
@Slf4j
public class FastDFSClient {
    private static TrackerClient trackerClient = null;
    private static TrackerServer trackerServer = null;
    private static StorageServer storageServer = null;
    private static StorageClient1 client = null;
    // fdfs client的配置文件的路径
    private static String TRACKER_CLIENT_CONF = "/fdfs_client.conf";

    static {
        try {
            // 配置文件必须指定全路径
            String confName = FastDFSClient.class.getResource(TRACKER_CLIENT_CONF).getPath();
            // 配置文件全路径中如果有中文，需要进行utf8转码
            confName = URLDecoder.decode(confName, UTF_8);
            ClientGlobal.init(confName);
            trackerClient = new TrackerClient();
            trackerServer = trackerClient.getConnection();
            storageServer = null;
            client = new StorageClient1(trackerServer, storageServer);
        } catch (Exception e) {
            log.error("FastDFSClient error", e);
        }
    }

    /**
     * @param fileName 文件全路径
     * @param extName  文件扩展名，不包含(.)
     * @param metas    文件扩展信息
     * @return
     * @throws Exception
     */
    public static String uploadFile(String fileName, String extName, NameValuePair[] metas) throws Exception {
        return client.upload_file1(fileName, extName, metas);
    }

    public static void main(String[] args) {
        try {
            String file = uploadFile("/Users/yangwei/Desktop/picture/bj.jpg", "png", null);
            log.info("upload:" + file);
        } catch (Exception e) {
            log.error("upload file to FastDFS failed", e);
        }
    }
}
