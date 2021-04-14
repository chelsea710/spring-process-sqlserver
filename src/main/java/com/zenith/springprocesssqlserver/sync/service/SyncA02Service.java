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
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 2021/4/14
 */
@Service
public class SyncA02Service {
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
     * @param sqlA06Record 最后的记录
     * @param syncId 同步id
     */
    public void addOrEditA06(Record pgRecord, Record recordBefore, Record sqlA06Record, String syncId) {
        List<Record> a06RecordList = syncDao.findPgA06InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA06Record = this.processA06Contrast(a06RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA06Record)) {
            //修改
            this.updateA06(pgRecord, pgA06Record, sqlA06Record, a06RecordList, syncId);
        } else {
            //新增
            Db.use(DBConstant.PG).save("a06", "A0600", sqlA06Record);
            this.updateA01ZYInfo(pgRecord, sqlA06Record,a06RecordList,syncId);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "2", "1", "", "");
        }
    }


    /**
     * 修改a01的专业信息
     * @param pgRecord a01的人员信息
     * @param sqlA06Record 修改的A06List信息
     * @param a06RecordList 为了同步A01的A0196字段
     * @param syncId 同步id
     */
    public void updateA01ZYInfo(Record pgRecord,Record sqlA06Record,List<Record> a06RecordList,String syncId){
        String A0196 = CollectionUtil.join(a06RecordList.stream().peek(var -> {
            if (StrUtil.equals(var.getStr("A0600"), sqlA06Record.getStr("A0600"))) {
                var.set("A0602", sqlA06Record.getStr("A0602"));
            }
        }).map(var->var.getStr("A0602")).collect(Collectors.toList()), ",");
        Record updateA01Record = new Record();
        updateA01Record.set("A0000",pgRecord.getStr("A0000"));
        updateA01Record.set("A0196",A0196);
        Db.use(DBConstant.PG).update("a01","A0000",updateA01Record);
        syncA01Service.processSyncArchivesInfoList(pgRecord, updateA01Record, "a01", "专业技术信息集", pgRecord.getStr("A0000"), syncId);
    }

    /**
     *
     * @param pgRecord a01Record
     * @param pgA06Record 寻找到的要修改的Record
     * @param sqlA06Record 修改的值
     * @param a06RecordList 人员a06信息
     * @param syncId 同步id
     */
    public void updateA06(Record pgRecord,Record pgA06Record,Record sqlA06Record,List<Record> a06RecordList,String syncId){
        sqlA06Record.set("A0600", pgA06Record.getStr("A0600"));
        if (syncA01Service.processSyncArchivesInfoList(pgA06Record, sqlA06Record, "a06", "专业技术职务信息", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a06", "A0600", sqlA06Record);
            this.updateA01ZYInfo(pgRecord,sqlA06Record,a06RecordList,syncId);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA06(String a0184,Record recordBefore,Record record){
        Db.use(DBConstant.PG).tx(()-> {
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
            int index = 0;
            for (Record pgRecord : pgDataSource) {
                if(index != 0 ){
                    syncId = StrKit.getRandomUUID().toUpperCase();
                }
                Record sqlA06Record = new Record();
                syncService.setRecordA06(record, sqlA06Record,pgRecord.getStr("A0000"),syncDao.getDicMap("GB8561"));
                List<String> errorMsgList = verifyService.verifyA06(sqlA06Record);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA06(pgRecord,recordBefore, sqlA06Record, syncId);
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
    public void processByDeleteA02(String a0184,Record record){
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
                        List<Record> a02RecordList = syncDao.findPgA02InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA02Record = new Record();
                        Record updateA01Record = new Record();
                        syncService.setRecordA02(record, sqlA02Record,updateA01Record,pgRecord.getStr("A0000"), syncDao.getDicMap("GB8561"));
                        Record pgA02Record = this.processA02Contrast(a02RecordList, sqlA02Record);
                        if (ObjectUtil.isNotNull(pgA02Record)) {
                            //如果是不需要确认同步的话
                            if (!isSure) {
//                                syncDao.deletePGDataSource("a06", "A0600", pgA06Record.getStr("A0600"));
//                                //修改下A01
//                                String a0196 = CollectionUtil.join(a06RecordList.stream().filter(var -> StrUtil.equals(var.getStr("A0600"), pgA06Record.getStr("A0600")) && StrUtil.isNotEmpty(var.getStr("A0602")))
//                                        .map(var -> var.getStr("A0602")).collect(Collectors.toList()), ",");
//                                Record a01UpdateRecord = new Record();
//                                a01UpdateRecord.set("A0000",pgRecord.getStr("A0000"));
//                                a01UpdateRecord.set("A0196",a0196);
//                                Db.use(DBConstant.PG).update("a01","A0000",a01UpdateRecord);
//                                Db.use(DBConstant.PG).save("a06_fuling_delete", pgA06Record.set("A0196",a0196));
//                                syncA01Service.saveDeleteSync(syncId, a0184, "", pgRecord.getStr("A0101"), "1", "1", "删除" + pgRecord.getStr("A0101") + "的专业技术职务信息", "a06");
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
     * 处理a02的对比问题
     * @param a02RecordList a02的集合
     * @param sqlA02Record sqlServer中的a02
     * @return 对比出来的a02
     */
    private Record processA02Contrast(List<Record> a02RecordList, Record sqlA02Record) {
        //需要用编码来进行对比
        return null;
    }

    /**
     * 删除对比下是哪个专业技术职务被删除了
     * @param a06RecordList 人员所有的a06信息
     * @param sqlA06Record sqlserver同步过来的a06信息
     * @return 对比出来的删除的哪一个
     */
    private Record processA06Contrast(List<Record> a06RecordList, Record sqlA06Record) {
        return a06RecordList.stream().filter(var->StrUtil.equals(var.getStr("A0601"),sqlA06Record.getStr("A0601"))).findFirst().orElse(null);
    }
}
