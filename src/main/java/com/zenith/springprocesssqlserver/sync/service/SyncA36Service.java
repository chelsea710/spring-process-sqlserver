package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
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
public class SyncA36Service {
    @Autowired
    private SyncDao syncDao;

    @Autowired
    private VerifyService verifyService;

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
     * 新增或者修改A01 逻辑一致
     *
     * @param sqlA36Record 最后的记录
     * @param syncId       同步id
     */
    public void addOrEditA36(Record pgRecord, Record recordBefore, Record sqlA36Record, String syncId) {
        List<Record> a36RecordList = syncDao.findPgA36InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA36Record = this.processA36Contrast(a36RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA36Record)) {
            //修改
            this.updateA36(pgRecord, pgA36Record, sqlA36Record, syncId);
        } else {
            //新增
            Db.use(DBConstant.PG).save("a36", "A3600", sqlA36Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "2", "1", "", "");
        }
    }

    /**
     * @param pgRecord     a01Record
     * @param pgA15Record  寻找到的要修改的Record
     * @param sqlA15Record 修改的值
     * @param syncId       同步id
     */
    public void updateA36(Record pgRecord, Record pgA15Record, Record sqlA15Record, String syncId) {
        sqlA15Record.set("A1500", sqlA15Record.getStr("A1500"));
        if (syncA01Service.processSyncArchivesInfoList(pgA15Record, sqlA15Record, "a15", "年度考核信息级", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a15", "A1500", sqlA15Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a36的操作
     *
     * @param a0184  人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA36(String a0184, Record recordBefore, Record record) {
        Db.use(DBConstant.PG).tx(() -> {
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
            int index = 0;
            for (Record pgRecord : pgDataSource) {
                if (index != 0) {
                    syncId = StrKit.getRandomUUID().toUpperCase();
                }
                Record sqlA36Record = new Record();
                syncService.setRecordA36(record, sqlA36Record, pgRecord.getStr("A0000"), syncService.getA3604Map());
                List<String> errorMsgList = verifyService.verifyA36(sqlA36Record,pgRecord);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA36(pgRecord, recordBefore, sqlA36Record, syncId);
                    }
                } else {
                    //3:失败就反结果
                    syncA01Service.saveDeleteSync(syncId, a0184, "", pgRecord.getStr("A0101"), "2", "0", CollectionUtil.join(errorMsgList, "\n"), "");
                }
                index++;
            }
            return true;
        });
    }

    /**
     * 处理删除a01的操作
     *
     * @param a0184  人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA36(String a0184, Record record) {
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
                        List<Record> a36RecordList = syncDao.findPgA36InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA36Record = new Record();
                        syncService.setRecordA36(record, sqlA36Record, pgRecord.getStr("A0000"), syncService.getA3604Map());
                        Record pgA36Record = this.processA36Contrast(a36RecordList, sqlA36Record);
                        if (ObjectUtil.isNotNull(pgA36Record)) {
                            //如果是不需要确认同步的话
                            if (!isSure) {
                                syncDao.deletePGDataSource("a36", "A3600", pgA36Record.getStr("A3600"));
                                Db.use(DBConstant.PG).save("a36_fuling_delete", pgA36Record);
                                syncA01Service.saveDeleteSync(syncId, a0184, pgA36Record.getStr("A0000"), pgRecord.getStr("A0101"), "1", "1", "删除" + pgRecord.getStr("A0101") + "的家庭成员信息", "a14");
                            }
                        }
                        index++;
                    }
                }
            }
            return true;
        });
    }

    /**
     * 对比a36的数据
     */
    private Record processA36Contrast(List<Record> a36RecordList, Record sqlA36Record) {
        return a36RecordList.stream().filter(var -> ObjectUtil.equal(var.getDate("A3604A"), sqlA36Record.getDate("A3604A"))).findFirst().orElse(null);
    }
}