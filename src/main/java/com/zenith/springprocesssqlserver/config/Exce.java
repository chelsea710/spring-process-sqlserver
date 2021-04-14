package com.zenith.springprocesssqlserver.config;

import com.jfinal.plugin.activerecord.Db;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.sync.service.SyncService;
import com.zenith.springprocesssqlserver.sync.service.SyncSingleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;


/**
 * @author LHR
 * @date 20210326
 */
@Component
@Order(1)
public class Exce implements ApplicationRunner {

    @Autowired
    private SyncService syncService;

    @Autowired
    private SyncSingleService syncSingleService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        syncService.syncA01();
//        syncService.syncA02();
//        syncService.syncA05();
//        syncService.syncA06();
//        syncService.syncA08();
//        syncService.syncA14();
//        syncService.syncA15();
//        syncService.syncA36();
//        syncService.syncB01();
//        syncService.syncphotos();
//        syncService.processQxRole();
        //同步一下身份证和A0000
        syncService.syncLeaderCode();
        //开始同步
        while (true){
            Thread.sleep(500);
            syncSingleService.sync();
        }


    }




}
