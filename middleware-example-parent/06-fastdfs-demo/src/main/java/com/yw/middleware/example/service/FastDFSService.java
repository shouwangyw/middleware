package com.yw.middleware.example.service;

import com.yw.middleware.example.model.FastDFSFile;
import org.csource.fastdfs.FileInfo;
import org.springframework.web.multipart.MultipartFile;

import java.util.Optional;

/**
 * @author yangwei
 */
public interface FastDFSService {
    /**
     * 文件上传
     */
    Optional<String> uploadFile(MultipartFile file);

    /**
     * 文件上传
     */
    Optional<String> uploadFile(FastDFSFile file);

    /**
     * 文件下载
     */
    void downloadFile(String groupName, String remoteFileName);

    /**
     * 文件删除
     */
    boolean deleteFile(String groupName, String remoteFileName) throws Exception;

    /**
     * 根据 groupName 和文件名获取文件信息
     */
    Optional<FileInfo> getFileInfo(String groupName, String remoteFileName);
}
