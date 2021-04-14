package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.gbservice.A0000Dto;
import com.zenith.springprocesssqlserver.gbservice.BatchUpdateMem;
import com.zenith.springprocesssqlserver.gbservice.InMessage;
import com.zenith.springprocesssqlserver.gbservice.MemService;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author LHR
 * @date 2021/4/13
 */
@Service
public class SyncA15Service {
    @Autowired
    private SyncDao syncDao;

    @Autowired
    private VerifyService verifyService;

    @Autowired
    private SyncService syncService;

    @Autowired
    private SyncA01Service syncA01Service;

    @Autowired
    private MemService memService;

    /**
     * 为以后如果需要点击是否同步所做的处理
     */
    @Value("${sync.isSure}")
    private Boolean isSure;

    /**
     * 新增或者修改A01 逻辑一致
     * @param sqlA14Record 最后的记录
     * @param syncId 同步id
     */
    public void addOrEditA15(Record pgRecord, Record recordBefore, Record sqlA14Record, String syncId) {
        List<Record> a14RecordList = syncDao.findPgA15InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA15Record = this.processA15Contrast(a14RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA15Record)) {
            //修改
            this.updateA15(pgRecord, pgA15Record, sqlA14Record, syncId);
        } else {
            //新增
            Db.use(DBConstant.PG).save("a15", "A1500", sqlA14Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "2", "1", "", "");
        }
    }

    /**
     *
     * @param pgRecord a01Record
     * @param pgA15Record 寻找到的要修改的Record
     * @param sqlA15Record 修改的值
     * @param syncId 同步id
     */
    public void updateA15(Record pgRecord,Record pgA15Record,Record sqlA15Record,String syncId){
        sqlA15Record.set("A1500", sqlA15Record.getStr("A1500"));
        if (syncA01Service.processSyncArchivesInfoList(pgA15Record, sqlA15Record, "a15", "年度考核信息级", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a15", "A1500", sqlA15Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA15(String a0184,Record recordBefore,Record record)  {
        List<String> replaceA0000 = new ArrayList<>();
        Db.use(DBConstant.PG).tx(()-> {
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
            int index = 0;
            for (Record pgRecord : pgDataSource) {
                if(index != 0 ){
                    syncId = StrKit.getRandomUUID().toUpperCase();
                }
                Record sqlA15Record = new Record();
                syncService.setRecordA15(record, sqlA15Record,pgRecord.getStr("A0000"), syncService.zwccMap());
                List<String> errorMsgList = verifyService.verifyA15(sqlA15Record);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA15(pgRecord, recordBefore,sqlA15Record, syncId);
                        replaceA0000.add(pgRecord.getStr("A0000"));
                    }
                } else {
                    //3:失败就反结果
                    syncA01Service.saveDeleteSync(syncId, a0184, "", pgRecord.getStr("A0101"), "2", "0", CollectionUtil.join(errorMsgList, "\n"), "");
                }
                index++;
            }
            return true;
        });
        this.sendBatchSyncMemByA15(replaceA0000);
    }

    /**
     * 处理删除a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA15(String a0184,Record record){
        List<String> replaceA0000 = new ArrayList<>();
        Db.use(DBConstant.PG).tx(()-> {
            String syncId = StrKit.getRandomUUID().toUpperCase();
            if (StrUtil.isNotEmpty(a0184)) {
                List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
                if (pgDataSource.size() > 0) {
                    int index = 0;
                    for (Record pgRecord : pgDataSource) {
                        if (index != 0) {
                            syncId = StrKit.getRandomUUID().toUpperCase();
                        }
                        List<Record> a15RecordList = syncDao.findPgA15InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA15Record = new Record();
                        syncService.setRecordA15(record, sqlA15Record, pgRecord.getStr("A0000"), syncService.a1517Map());
                        Record pgA15Record = this.processA15Contrast(a15RecordList, sqlA15Record);
                        if (ObjectUtil.isNotNull(pgA15Record)) {
                            //如果是不需要确认同步的话
                            if (!isSure) {
                                syncDao.deletePGDataSource("a15", "A1500", pgA15Record.getStr("A1500"));
                                replaceA0000.add(pgRecord.getStr("A0000"));
                                Db.use(DBConstant.PG).save("a15_fuling_delete", pgA15Record.set("A15Z104",pgRecord.getStr("A15Z101")));
                                syncA01Service.saveDeleteSync(syncId, a0184, pgA15Record.getStr("A0000"), pgRecord.getStr("A0101"), "1", "1", "删除" + pgRecord.getStr("A0101") + "的考核信息", "a14");
                            }
                        }
                        index++;
                    }
                }
            }
            return true;
        });
        //修改学历学位综述的问题
        this.sendBatchSyncMemByA15(replaceA0000);
    }

    private Record processA15Contrast(List<Record> a15RecordList, Record sqlA15Record) {
        return a15RecordList.stream().filter(var->ObjectUtil.equal(var.getDate("A1521"),sqlA15Record.getDate("A1521"))).findFirst().orElse(null);
    }

    /**
     * 发送批量重置学历学位标识的请求
     * @param replaceA0000 需要同步的a0000集合
     */
    public void sendBatchSyncMemByA15(List<String> replaceA0000){
        if(replaceA0000.size() > 0) {
            InMessage<BatchUpdateMem> batchUpdateMemInMessage = new InMessage<>();
            BatchUpdateMem batchUpdateMem = new BatchUpdateMem();
            batchUpdateMem.setIsA15("true");
            List<A0000Dto> param = new ArrayList<>();
            for (String a0000 : replaceA0000) {
                A0000Dto a0000Dto = new A0000Dto();
                a0000Dto.setA0000(a0000);
                param.add(a0000Dto);
            }
            batchUpdateMem.setMems(param);
            batchUpdateMemInMessage.setData(batchUpdateMem);
            memService.batchSyncMem(batchUpdateMemInMessage);
        }
    }
}
