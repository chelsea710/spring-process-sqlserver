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
public class SyncA14Service {

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
    public void addOrEditA14(Record pgRecord,Record recordBefore,Record sqlA14Record,String syncId) {
        List<Record> a14RecordList = syncDao.findPgA14InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA14Record = this.processA14Contrast(a14RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA14Record)) {
            //修改
            this.updateA14(pgRecord, pgA14Record, sqlA14Record, syncId);
        } else {
            //新增
            Db.use(DBConstant.PG).save("a14", "A1400", sqlA14Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "2", "1", "", "");
        }
    }

    /**
     *
     * @param pgRecord a01Record
     * @param pgA14Record 寻找到的要修改的Record
     * @param sqlA14Record 修改的值
     * @param syncId 同步id
     */
    public void updateA14(Record pgRecord,Record pgA14Record,Record sqlA14Record,String syncId){
        sqlA14Record.set("A1400", pgA14Record.getStr("A1400"));
        if (syncA01Service.processSyncArchivesInfoList(pgA14Record, sqlA14Record, "a14", "奖惩信息集", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a14", "A1400", sqlA14Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA14(String a0184,Record recordBefore,Record record)  {
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
                Record sqlA14Record = new Record();
                syncService.setRecordA14(record, sqlA14Record,pgRecord.getStr("A0000"), syncService.zwccMap());
                List<String> errorMsgList = verifyService.verifyA14(sqlA14Record);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA14(pgRecord, recordBefore,sqlA14Record, syncId);
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
        this.sendBatchSyncMemByA14(replaceA0000);
    }

    /**
     * 处理删除a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA14(String a0184,Record record){
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
                        List<Record> a14RecordList = syncDao.findPgA14InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA14Record = new Record();
                        syncService.setRecordA14(record, sqlA14Record, pgRecord.getStr("A0000"), syncService.zwccMap());
                        Record pgA14Record = this.processA14Contrast(a14RecordList, sqlA14Record);
                        if (ObjectUtil.isNotNull(pgA14Record)) {
                            //如果是不需要确认同步的话
                            if (!isSure) {
                                syncDao.deletePGDataSource("a14", "A1400", pgA14Record.getStr("A1400"));
                                replaceA0000.add(pgRecord.getStr("A0000"));
                                Db.use(DBConstant.PG).save("a14_fuling_delete", pgA14Record.set("A14Z101",pgRecord.getStr("A14Z101")));
                                syncA01Service.saveDeleteSync(syncId, a0184, pgA14Record.getStr("A0000"), pgRecord.getStr("A0101"), "1", "1", "删除" + pgRecord.getStr("A0101") + "的奖惩信息", "a14");
                            }
                        }
                        index++;
                    }
                }
            }
            return true;
        });
        //修改学历学位综述的问题
        this.sendBatchSyncMemByA14(replaceA0000);
    }

    /**
     * 对比奖惩信息
     * @param a14RecordList 人员奖惩集合信息
     * @param sqlA14Record 保存奖惩信息
     * @return
     */
    private Record processA14Contrast(List<Record> a14RecordList, Record sqlA14Record) {
        return a14RecordList.stream().filter(var->StrUtil.equals(var.getStr("A1404B"),sqlA14Record.getStr("A1404B"))).findFirst().orElse(null);
    }


    /**
     * 发送批量重置学历学位标识的请求
     * @param replaceA0000 需要同步的a0000集合
     */
    public void sendBatchSyncMemByA14(List<String> replaceA0000){
        if(replaceA0000.size() > 0) {
            InMessage<BatchUpdateMem> batchUpdateMemInMessage = new InMessage<>();
            BatchUpdateMem batchUpdateMem = new BatchUpdateMem();
            batchUpdateMem.setIsA14("true");
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
