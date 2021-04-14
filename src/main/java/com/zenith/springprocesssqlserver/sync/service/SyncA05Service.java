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

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 2021/4/11
 */
@Service
public class SyncA05Service {
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
     * @param sqlA05Record 最后的记录
     * @param syncId 同步id
     */
    public void addOrEditA05(Record pgRecord,Record recordBefore,Record sqlA05Record,String syncId) {
        List<Record> a05RecordList = syncDao.findPgA05InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA05Record = this.processA05ContrastForUpdateOrAdd(a05RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA05Record)) {
            //修改
            this.updateA05(pgRecord,pgA05Record,sqlA05Record,syncId);
        } else {
            //新增
            String a0531 = sqlA05Record.getStr("A0531");
            Optional<Record> optional = a05RecordList.stream().filter(var -> StrUtil.equalsAny(var.getStr("A0531"), a0531)).findFirst();
            if(optional.isPresent()){
                this.updateA05(pgRecord,optional.get(),sqlA05Record,syncId);
            } else {
                Db.use(DBConstant.PG).save("a05","A0500",sqlA05Record);
                this.updateA01ZJInfo(pgRecord,sqlA05Record,syncId);
                syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"),pgRecord.getStr("A0101"), "2", "1", "", "");
            }
        }
    }


    public void updateA01ZJInfo(Record pgRecord,Record sqlA05Record,String syncId){
        if (StrUtil.equalsAny(sqlA05Record.getStr("A0531"), "0")) {
            Record updateA01Record = new Record();
            updateA01Record.setColumns(pgRecord.getColumns());
            updateA01Record.set("A0221", sqlA05Record.getStr("A0501B"));
            updateA01Record.set("A0288", sqlA05Record.getDate("A0504"));
            syncA01Service.processSyncArchivesInfoList(pgRecord, updateA01Record, "a01", "职务职级信息", pgRecord.getStr("A0000"), syncId);
            Db.use(DBConstant.PG).update("a01", "A0000", updateA01Record);
        } else if (StrUtil.equalsAny(sqlA05Record.getStr("A0531"), "1")) {
            Record updateA01Record = new Record();
            updateA01Record.setColumns(pgRecord.getColumns());
            updateA01Record.set("A0192E", sqlA05Record.getStr("A0501B"));
            syncA01Service.processSyncArchivesInfoList(pgRecord, updateA01Record, "a01", "职务职级信息", pgRecord.getStr("A0000"), syncId);
            Db.use(DBConstant.PG).update("a01", "A0000", updateA01Record);
        }
    }

    /**
     *
     * @param pgRecord a01Record
     * @param pgA05Record 寻找到的要修改的Record
     * @param sqlA05Record 修改的值
     * @param syncId 同步id
     */
    public void updateA05(Record pgRecord,Record pgA05Record,Record sqlA05Record,String syncId){
        sqlA05Record.set("A0500", pgA05Record.getStr("A0500"));
        if (syncA01Service.processSyncArchivesInfoList(pgA05Record, sqlA05Record, "a05", "职务职级信息", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a05", "A0500", sqlA05Record);
            this.updateA01ZJInfo(pgRecord,sqlA05Record,syncId);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA05(String a0184,Record recordBefore,Record record){
        Db.use(DBConstant.PG).tx(()-> {
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
            int index = 0;
            for (Record pgRecord : pgDataSource) {
                if(index != 0 ){
                    syncId = StrKit.getRandomUUID().toUpperCase();
                }
                Record sqlA05Record = new Record();
                syncService.setRecordA05(record, sqlA05Record,pgRecord.getStr("A0000"), syncService.zwccMap(), syncService.zwzjMap());
                List<String> errorMsgList = verifyService.verifyA05(sqlA05Record);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA05(pgRecord,recordBefore, sqlA05Record, syncId);
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
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA05(String a0184,Record record){
        Db.use(DBConstant.PG).tx(()-> {
            String syncId = StrKit.getRandomUUID().toUpperCase();
            if (StrUtil.isNotEmpty(a0184)) {
                List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
                if (pgDataSource.size() > 0) {
                    int index = 0;
                    for (Record pgRecord : pgDataSource) {
                        if(index != 0){
                            syncId = StrKit.getRandomUUID().toUpperCase();
                        }
                        List<Record> a05RecordList = syncDao.findPgA05InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA05Record = new Record();
                        syncService.setRecordA05(record, sqlA05Record,pgRecord.getStr("A0000"), syncService.zwccMap(), syncService.zwzjMap());
                        Record pgA05Record = this.processA05Contrast(a05RecordList, sqlA05Record);
                        if (ObjectUtil.isNotNull(pgA05Record)) {

                            //如果是不需要确认同步的话
                            if (!isSure) {
                                syncDao.deletePGDataSource("a05", "A0500", pgA05Record.getStr("A0500"));
                                if(StrUtil.equals(sqlA05Record.getStr("A0531"),"0")){
                                    Record updateA01Record = new Record();
                                    updateA01Record.set("A0000",pgRecord.getStr("A0000"));
                                    updateA01Record.set("A0221",null);
                                    updateA01Record.set("A0288",null);
                                    Db.use(DBConstant.PG).update("a01","A0000",updateA01Record);
                                }
                                if(StrUtil.equals(sqlA05Record.getStr("A0531"),"1")){
                                    Record updateA01Record = new Record();
                                    updateA01Record.set("A0000",pgRecord.getStr("A0000"));
                                    updateA01Record.set("A0192E",null);
                                    updateA01Record.set("A0192C",null);
                                    Db.use(DBConstant.PG).update("a01","A0000",updateA01Record);
                                }
                            }

                            pgA05Record.set("syncId",syncId);
                            pgA05Record.set("A0221",pgRecord.getStr("A0221"));
                            pgA05Record.set("A0192E",pgRecord.getStr("A0192E"));
                            pgA05Record.set("A0288",pgRecord.getDate("A0288"));
                            pgA05Record.set("A0192C",pgRecord.getDate("A0192C"));

                            Db.use(DBConstant.PG).save("a05_fuling_delete", pgA05Record);
                            //记录日志表
                            syncA01Service.saveDeleteSync(syncId, a0184, "", pgRecord.getStr("A0101"), "1", "1", "删除" + record.getStr("A0101") + "的职务职级信息", "a01");
                        }
                        index++;
                    }
                }
            }
            return true;
        });
    }

    /**
     * 对比一下是哪一个a05
     * @param a05RecordList pg的a05所有数据
     * @param sqlA05Record sqlserver 触发过来的a05数据
     * @return 对比出来的pga05数据
     */
    private Record processA05Contrast(List<Record> a05RecordList, Record sqlA05Record) {
        String a0531 = sqlA05Record.getStr("A0531");
        String a0501B = sqlA05Record.getStr("A0501B");
        Optional<Record> first = a05RecordList.stream().filter(var -> StrUtil.equals(var.getStr("A0531"), a0531) && StrUtil.equals(var.getStr("A0501B"), a0501B)).findFirst();
        return first.orElse(null);
    }

    /**
     * 对比一下是哪一个a05
     * @param a05RecordList pg的a05所有数据
     * @param sqlA05Record sqlserver 触发过来的a05数据
     * @return 对比出来的pga05数据
     */
    private Record processA05ContrastForUpdateOrAdd(List<Record> a05RecordList, Record sqlA05Record) {
        String a0501B = sqlA05Record.getStr("A0501B");
        Optional<Record> first = a05RecordList.stream().filter(var -> StrUtil.equals(var.getStr("A0501B"), a0501B)).findFirst();
        return first.orElse(null);
    }
}
