package com.yw.middleware.example.service;

import com.yw.middleware.example.model.FastDFSFile;
import lombok.extern.slf4j.Slf4j;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * @author yangwei
 */
@Slf4j
@Service
public class FastDFSServiceImpl implements FastDFSService {
    /**
     * fdfs client的配置文件的路径
     */
    private static final String TRACKER_CLIENT_CONF = "fdfs_client.conf";

    private static TrackerClient trackerClient = null;
    private static TrackerServer trackerServer = null;
    private static StorageServer storageServer = null;
    private static StorageClient storageClient = null;

    @PostConstruct
    public void init() {
        try {
            String confPath = new ClassPathResource(TRACKER_CLIENT_CONF).getFile().getAbsolutePath();
            ClientGlobal.init(confPath);
            trackerClient = new TrackerClient();
            trackerServer = trackerClient.getConnection();
            storageServer = trackerClient.getStoreStorage(trackerServer);
            storageClient = new StorageClient(trackerServer, storageServer);
        } catch (Exception e) {
            log.error("FastDFS Client Init Fail!");
        }
    }

    @Override
    public Optional<String> uploadFile(MultipartFile file) {
        if (file.isEmpty()) {
            return Optional.empty();
        }
        try {
            String fileName = file.getOriginalFilename();
            String fileExt = fileName.substring(fileName.lastIndexOf(".") + 1);
            byte[] fileBuff = null;
            try (InputStream inputStream = file.getInputStream()) {
                int length = inputStream.available();
                fileBuff = new byte[length];
                inputStream.read(fileBuff);
            } catch (IOException e) {
                e.printStackTrace();
            }
            FastDFSFile fastDFSFile = new FastDFSFile(fileName, fileExt, fileBuff);
            Optional<String> optional = uploadFile(fastDFSFile);
            if (optional.isPresent()) {
                return Optional.of(getStorageUrl() + optional.get());
            }
        } catch (Exception e) {
            log.error("upload file failed", e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> uploadFile(FastDFSFile file) {
        log.info("File Name: " + file.getFileName() + "File Length:" + file.getFileContent().length);

        NameValuePair[] metaData = new NameValuePair[1];
        metaData[0] = new NameValuePair("author", file.getAuthor());

        long startTime = System.currentTimeMillis();
        try {
            if (storageClient != null) {
                String[] results = storageClient.upload_file(file.getFileContent(), file.getFileExt(), metaData);
                log.info("upload_file time used:" + (System.currentTimeMillis() - startTime) + " ms");
                if (results != null) {
                    return Optional.of(results[0] + "/" + results[1]);
                }
            }
        } catch (Exception e) {
            log.error("upload file error : " + e);
        }
        return Optional.empty();
    }

    @Override
    public void downloadFile(String groupName, String remoteFileName) {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("/Users/yangwei/Desktop/download.jpg"))) {
            byte[] fileBytes = storageClient.download_file(groupName, remoteFileName);

            bos.write(fileBytes);
        } catch (Exception e) {
            log.error("download file error : " + e);
        }
    }

    @Override
    public boolean deleteFile(String groupName, String remoteFileName) throws Exception {
        int result = storageClient.delete_file(groupName, remoteFileName);
        return result > 0;
    }

    @Override
    public Optional<FileInfo> getFileInfo(String groupName, String remoteFileName) {
        try {
            FileInfo fileInfo = storageClient.get_file_info(groupName, remoteFileName);
            Optional.of(fileInfo);
        } catch (Exception e) {
            log.error("get file info failed : ", e);
        }
        return Optional.empty();
    }

    private String getStorageUrl() {
        return "http://" + storageServer.getInetSocketAddress().getHostString() + ":" + ClientGlobal.getG_tracker_http_port() + "/";
    }
}
