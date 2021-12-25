package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.util.StrUtil;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author LHR
 * @date 2021/4/13
 */
@Service
public class SyncA44Service {
    @Autowired
    private SyncDao syncDao;

    @Autowired
    private SyncService syncService;

    @Autowired
    private SyncA01Service syncA01Service;

    /**
     * 为以后如果需要点击是否同步所做的处理
     */
    @Value("${sync.isSure}")
    private Boolean isSure;


    /**
     * 处理删除a01的操作
     *
     * @param a0184  人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddOrUpdateOrDeleteA44(String a0184, Record record) {
        Db.use(DBConstant.PG).tx(() -> {
            String syncId = StrKit.getRandomUUID().toUpperCase();
            if (StrUtil.isNotEmpty(a0184)) {
                List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
                if (pgDataSource.size() > 0) {
                    int index = 0;
                    for (Record pgRecord : pgDataSource) {
                        if (index != 0) {
                            syncId = StrKit.getRandomUUID().toUpperCase();
                        }
                        if(!isSure) {
                            List<Record> a44RecordList = syncDao.findA44List(record.getStr("leader_code"));
                            Record sqlA44Record = new Record();
                            syncService.setRecordA44(a44RecordList, sqlA44Record, pgRecord.getStr("A0000"));
                            Db.use(DBConstant.PG).update("a01", "A0000", sqlA44Record);
                            syncA01Service.processSyncArchivesInfoList(pgRecord, sqlA44Record, "a44", "简历信息集", pgRecord.getStr("A0000"), syncId);
                            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
                        }
                        index++;
                    }
                }
            }
            return true;
        });
    }
}
