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
 * @date 2021/4/12
 */
@Service
public class SyncA08Service {

    @Autowired
    private SyncDao syncDao;

    @Autowired
    private VerifyService verifyService;

    @Autowired
    private SyncService syncService;

    @Autowired
    private SyncA01Service syncA01Service;

    @Autowired
    private DictionaryService dictionaryService;

    @Autowired
    private MemService memService;

    /**
     * 为以后如果需要点击是否同步所做的处理
     */
    @Value("${sync.isSure}")
    private Boolean isSure;

    /**
     * 新增或者修改A01 逻辑一致
     * @param sqlA08Record 最后的记录
     * @param syncId 同步id
     */
    public void addOrEditA08(Record pgRecord,Record recordBefore,Record sqlA08Record,String syncId) {
        List<Record> a08RecordList = syncDao.findPgA08InfoByA0000(pgRecord.getStr("A0000"));
        Record pgA08Record = this.processA08Contrast(a08RecordList, recordBefore);
        if (ObjectUtil.isNotNull(pgA08Record)) {
            //修改
            this.updateA08(pgRecord, pgA08Record, sqlA08Record, syncId);
        } else {
            //新增
            Db.use(DBConstant.PG).save("a08", "A0800", sqlA08Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "2", "1", "", "");
        }
    }

    /**
     *
     * @param pgRecord a01Record
     * @param pgA08Record 寻找到的要修改的Record
     * @param sqlA08Record 修改的值
     * @param syncId 同步id
     */
    public void updateA08(Record pgRecord,Record pgA08Record,Record sqlA08Record,String syncId){
        sqlA08Record.set("A0800", pgA08Record.getStr("A0800"));
        if (syncA01Service.processSyncArchivesInfoList(pgA08Record, sqlA08Record, "a08", "学历信息集", pgRecord.getStr("A0000"), syncId)) {
            Db.use(DBConstant.PG).update("a08", "A0800", sqlA08Record);
            syncA01Service.saveDeleteSync(syncId, pgRecord.getStr("A0184"), pgRecord.getStr("A0000"), pgRecord.getStr("A0101"), "3", "1", "", "");
        }
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA08(String a0184,Record recordBefore,Record record)  {
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
                Record sqlA08Record = new Record();
                syncService.setRecordA08(record, sqlA08Record,pgRecord.getStr("A0000"), dictionaryService.getSet("ZB64"),dictionaryService.getDicMap("ZB64"));
                List<String> errorMsgList = verifyService.verifyA08(sqlA08Record);
                if (errorMsgList.size() == 0) {
                    //2:成功就新增
                    if (!isSure) {
                        this.addOrEditA08(pgRecord, recordBefore,sqlA08Record, syncId);
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
        this.sendBatchSyncMemByA08(replaceA0000);
    }

    /**
     * 处理删除a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA08(String a0184,Record record){
        List<String> replaceA0000 = new ArrayList<>();
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
                        List<Record> a08RecordList = syncDao.findPgA08InfoByA0000(pgRecord.getStr("A0000"));
                        Record sqlA08Record = new Record();
                        syncService.setRecordA08(record, sqlA08Record,pgRecord.getStr("A0000"), dictionaryService.getSet("ZB64"),dictionaryService.getDicMap("ZB64"));
                        Record pgA08Record = this.processA08Contrast(a08RecordList, sqlA08Record);
                        if (ObjectUtil.isNotNull(pgA08Record)) {
                            //如果是不需要确认同步的话
                            if (!isSure) {
                                syncDao.deletePGDataSource("a08", "A0800", pgA08Record.getStr("A0800"));
                                //修改下A01
                                //是否输出 A01的东西可能要重新生成
                                if(StrUtil.equals(pgA08Record.getStr("A0898"),"1")){
                                    replaceA0000.add(pgRecord.getStr("A0000"));
                                    //处理下学历在A08的输出学历信息调用 干部服务处理下学历输出信息
                                    syncA01Service.saveDeleteSync(syncId, a0184, pgA08Record.getStr("A0000"), pgRecord.getStr("A0101"), "1", "1", "删除" + pgRecord.getStr("A0101") + "的学历信息", "a08");
                                }
                            }
                        }
                        index++;
                    }
                }
            }
            return true;
        });
        //修改学历学位综述的问题
        this.sendBatchSyncMemByA08(replaceA0000);
    }


    /**
     * 发送批量重置学历学位标识的请求
     * @param replaceA0000 需要同步的a0000集合
     */
    public void sendBatchSyncMemByA08(List<String> replaceA0000){
        if(replaceA0000.size() > 0) {
            InMessage<BatchUpdateMem> batchUpdateMemInMessage = new InMessage<>();
            BatchUpdateMem batchUpdateMem = new BatchUpdateMem();
            batchUpdateMem.setIsA08("true");
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


    /**
     * 对比学历
     * @param a08RecordList 人员学历集合
     * @param sqlA08Record 修改的学历信息
     * @return 对比出来的学历对象
     */
    private Record processA08Contrast(List<Record> a08RecordList, Record sqlA08Record) {
        return a08RecordList.stream()
                .filter(var->StrUtil.equals(var.getStr("A0801B"),sqlA08Record.getStr("A0801B")) && StrUtil.equals(var.getStr("A0837"),sqlA08Record.getStr("A0837")) && ObjectUtil.equal(var.getDate("A0804"),sqlA08Record.getDate("A0804")))
                .findFirst().orElse(null);
    }

}
