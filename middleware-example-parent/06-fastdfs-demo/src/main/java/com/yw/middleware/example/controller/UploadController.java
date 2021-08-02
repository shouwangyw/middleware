package com.yw.middleware.example.controller;

import com.yw.middleware.example.service.FastDFSService;
import lombok.extern.slf4j.Slf4j;
import org.csource.fastdfs.FileInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.Optional;

/**
 * @author yangwei
 */
@Slf4j
@RestController
@RequestMapping("fdfs")
public class UploadController {
    @Autowired
    private FastDFSService fastDFSService;


    @PostMapping("upload")
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        Optional<String> optional = fastDFSService.uploadFile(file);
        return optional.orElse(null);
    }

    @RequestMapping("download")
    public void downloadFile(@RequestParam("groupName") String groupName,
                             @RequestParam("remoteFileName") String remoteFileName) {
        fastDFSService.downloadFile(groupName, remoteFileName);
    }

    @RequestMapping("delete")
    public String deleteFile(@RequestParam("groupName") String groupName,
                             @RequestParam("remoteFileName") String remoteFileName) {
        try {
            boolean result = fastDFSService.deleteFile(groupName, remoteFileName);
            if (result) {
                return "delete file successfully";
            }
        } catch (Exception e) {
            log.error("delete file error : " + e);
        }
        return "delete file failed";
    }

    @RequestMapping("info")
    public FileInfo getFileInfo(@RequestParam("groupName") String groupName,
                                @RequestParam("remoteFileName") String remoteFileName) {
        try {
            Optional<FileInfo> optional = fastDFSService.getFileInfo(groupName, remoteFileName);
            if (optional.isPresent()) {
                return optional.get();
            }
        } catch (Exception e) {
            log.error("delete file error : " + e);
        }
        return null;
    }
}
