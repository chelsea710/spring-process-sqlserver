package com.zenith.springprocesssqlserver.sync.controller;

import cn.hutool.core.thread.ThreadUtil;
import com.zenith.springprocesssqlserver.sync.service.SyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author LHR
 * @date 20210326
 */
@RestController
@RequestMapping("/v1")
public class SyncController {

    @Autowired
    private SyncService syncService;

    @RequestMapping("/findPic")
    public String findPic() throws Exception{
        return syncService.findPic();
    }

    @PostMapping("/syncSqlserver")
    public void syncSqlserver(@RequestBody Object o){
        ThreadUtil.newExecutor().execute(()->{
            System.out.println(o);
        });
    }

}
