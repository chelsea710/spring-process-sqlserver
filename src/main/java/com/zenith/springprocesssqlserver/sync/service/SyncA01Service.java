package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
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
public class SyncA01Service {


    @Autowired
    private SyncDao syncDao;

    @Autowired
    private VerifyService verifyService;

    @Autowired
    private SyncService syncService;

    /**
     * 为以后如果需要点击是否同步所做的处理
     */
    @Value("${sync.isSure}")
    private Boolean isSure;

    /**
     * 同步a01功能
     * @param cdcTableName 系统表名
     */
    public void processCount(String cdcTableName){
        //同步之后的删除数据
        List<Record> startLsnDeleteList = new ArrayList<>();
        List<Record> cdcTableList = syncDao.findCdcSystemTableList(cdcTableName);
        LinkedHashMap<Object, List<Record>> cdcTableMap = cdcTableList.stream().collect(Collectors.groupingBy(var -> cn.hutool.core.codec.Base64.encode(var.getBytes("__$update_mask")), LinkedHashMap::new, Collectors.toList()));
        for (Map.Entry<Object, List<Record>> entry : cdcTableMap.entrySet()) {
            if(entry.getValue().size() == 1){
                Record record = entry.getValue().get(0);
                String operation = record.getStr("__$operation");
                String a0184 = record.getStr("A0184");
                //删除a01数据?
                if(StrUtil.equalsAny(operation,"1")) {
                    this.processByDeleteA01(a0184,record);
                }
                //新增a01数据
                if(StrUtil.equalsAny(operation,"2")){
                    this.processByAddA01(a0184,record);
                }
                startLsnDeleteList.add(record);
            } else {
                //修改
                List<Record> value = entry.getValue();
                //sqlserver修改前的数据
                Record updateRecordBefore = value.stream().filter(var -> StrUtil.equals(var.getStr("__$operation"), "3")).findFirst().get();
                startLsnDeleteList.add(updateRecordBefore);
                //sqlserver修改后的数据
                Record updateRecordAfter = value.stream().filter(var -> StrUtil.equals(var.getStr("__$operation"), "4")).findFirst().get();
                startLsnDeleteList.add(updateRecordAfter);
                if(!StrUtil.equals(updateRecordAfter.getStr("a0184"),updateRecordBefore.getStr("a0184"))){
                    //修改了人员身份证
                    this.processByUpdateA01(updateRecordBefore.getStr("a0184"),updateRecordAfter,"1");
                } else {
                    this.processByUpdateA01(updateRecordAfter.getStr("a0184"),updateRecordAfter,"0");
                }
            }
        }
        if(startLsnDeleteList.size() > 0){
            for (Record record : startLsnDeleteList) {
                Db.use(DBConstant.SQLSERVER).delete(cdcTableName,"__$start_lsn",record);
            }
        }
    }


    /**
     * 同步a01修改处理
     * @param isUpdateA0184 是否修改了身份证 1:修改了 0:没有修改
     * @param newA0184 使用的最终A0184
     * @param updateRecordAfter 修改之后的Record
     */
    public void processByUpdateA01(String newA0184, Record updateRecordAfter, String isUpdateA0184){
        Db.use(DBConstant.PG).tx(()-> {
            if(StrUtil.equals(isUpdateA0184,"1")){
                syncDao.updateSyncA0184Mapping(updateRecordAfter.getStr("leader_code"),updateRecordAfter.getStr("a0184"));
            }
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<String> errorMsgList = verifyService.verifyA01(updateRecordAfter);
            if (errorMsgList.size() == 0) {
                //2:成功就新增 还要处理下变更字段问题
                if(!isSure) {
                    this.addOrEditA01(newA0184,updateRecordAfter,syncId);
                }
            } else {
                //3:失败就反结果
                this.saveDeleteSync(syncId, newA0184, "",updateRecordAfter.getStr("a0101"), "3", "0", CollectionUtil.join(errorMsgList, "\n"), "");
            }
            return true;
        });
    }

    /**
     * 新增或者修改A01 逻辑一致
     * @param newA0184 身份很证
     * @param updateRecordAfter 最后的记录
     * @param syncId 同步id
     */
    public void addOrEditA01(String newA0184,Record updateRecordAfter,String syncId){
        Record saveRecord = new Record();
        syncService.setRecordA01(updateRecordAfter, saveRecord);
        List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", newA0184);
        if(pgDataSource.size() > 0){
            List<Record> updateRecordList = new ArrayList<>();
            int index = 0;
            for (Record record1 : pgDataSource) {
                if(index != 0){
                    syncId = StrKit.getRandomUUID().toUpperCase();
                }
                Record updateRecord = new Record();
                updateRecord.setColumns(saveRecord.getColumns());
                updateRecord.set("A0000",record1.getStr("A0000"));
                //记录修改的东西 没有修改就不同步有修改才同步
                if(this.processSyncArchivesInfoList(record1,updateRecord,"a01","基本信息",updateRecord.getStr("A0000"),syncId)) {
                    updateRecordList.add(updateRecord);
                    this.saveDeleteSync(syncId, newA0184, updateRecord.getStr("A0000"),updateRecord.getStr("A0000"), "3", "1", "", "");
                }
                index++;
            }
            if(updateRecordList.size() > 0){
                Db.use(DBConstant.PG).batchUpdate("a01","A0000",updateRecordList,1000);
            }
        } else {
            Db.use(DBConstant.PG).save("a01", "A0000", saveRecord);
            this.saveDeleteSync(syncId, newA0184, saveRecord.getStr("A0000"),saveRecord.getStr("A0101"), "2", "1", "", "");
        }
    }

    /**
     * 处理syncArchivesInfoList表
     * @param updateRecordBefore 修改之的对象
     * @param updateRecordAfter 修改之后的对象
     * @param table 表名
     * @param tableName 表对应的中文名字
     * @param primaryKey 修改的主键
     * @param syncId 同步信息表的id
     * @return 是否应该去修改他
     */
    public boolean processSyncArchivesInfoList(Record updateRecordBefore, Record updateRecordAfter, String table, String tableName,String primaryKey,String syncId) {
        List<Record> syncArchivesInfoList = new ArrayList<>();
        Map<String, String> comparedMap = this.getComparedMap(table);
        for (Map.Entry<String, String> entry : comparedMap.entrySet()) {
            String field = entry.getKey().split(",")[0];
            String fieldName = entry.getKey().split(",")[1];
            String fieldType = entry.getValue();
            if(StrUtil.equals(fieldType,"varchar")){
                if(!StrUtil.equals(updateRecordAfter.getStr(field),updateRecordBefore.getStr(field))){
                    String oldValue = StrUtil.isNotEmpty(updateRecordBefore.getStr(field))?updateRecordBefore.getStr(field):"";
                    String value = StrUtil.isNotEmpty(updateRecordAfter.getStr(field))?updateRecordAfter.getStr(field):"";
                    syncArchivesInfoList.add(this.saveSyncArchiveInfo(syncId,tableName,field,value,oldValue,fieldName,tableName,fieldType,primaryKey));
                }
            } else if (StrUtil.equals(fieldType,"timestamp")){
                if(!ObjectUtil.equal(updateRecordAfter.getDate(field),updateRecordBefore.getDate(field))){
                    String oldValue = ObjectUtil.isNotNull(updateRecordBefore.getDate(field))?DateUtil.format(updateRecordBefore.getDate(field),"yyyy.MM"):"";
                    String value = ObjectUtil.isNotNull(updateRecordAfter.getDate(field))?DateUtil.format(updateRecordAfter.getDate(field),"yyyy.MM"):"";
                    syncArchivesInfoList.add(this.saveSyncArchiveInfo(syncId,tableName,field,value,oldValue,fieldName,tableName,fieldType,primaryKey));
                }
            }
        }
        if(syncArchivesInfoList.size() > 0){
            Db.use(DBConstant.PG).batchSave("syncArchivesInfo",syncArchivesInfoList,100);
        }
        return syncArchivesInfoList.size() > 0;
    }


    /**
     * 保存同步详细信息
     * @param syncId 同步表的id
     * @param tableName 同步表的名字
     * @param fieldName 同步字段的名字
     * @param value 同步后的value
     * @param oldValue 同步之前的value
     * @param fieldChineseName 同步的字段中文名字
     * @param tableChineseName 同步的表中文名字
     * @param valueType 同步字段的类型
     * @param primaryKey 同步表的主键还原使用
     * @return 同步表的详细对象
     */
    public Record saveSyncArchiveInfo(String syncId,String tableName,String fieldName,String value,String oldValue,String fieldChineseName,String tableChineseName,String valueType,String primaryKey){
        Record record = new Record();
        record.set("id",StrKit.getRandomUUID().toUpperCase());
        record.set("syncId",syncId);
        record.set("tableName",tableName);
        record.set("fieldName",fieldName);
        record.set("fieldChineseName",fieldChineseName);
        record.set("tableChineseName",tableChineseName);
        record.set("valueType",valueType);
        record.set("value",value);
        record.set("oldValue",oldValue);
        record.set("primaryKey",primaryKey);
        return record;
    }

    /**
     * 处理新增a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByAddA01(String a0184,Record record){
        Db.use(DBConstant.PG).tx(()-> {
            //1:校核
            String syncId = StrKit.getRandomUUID().toUpperCase();
            List<String> errorMsgList = verifyService.verifyA01(record);
            if (errorMsgList.size() == 0) {
                //2:成功就新增
                if(!isSure) {
                    this.addOrEditA01(a0184,record,syncId);
                }
            } else {
                //3:失败就反结果
                this.saveDeleteSync(syncId, a0184,"", record.getStr("a0101"), "2", "0", CollectionUtil.join(errorMsgList, "\n"), "");
            }
            return true;
        });
    }

    /**
     * 处理删除a01的操作
     * @param a0184 人员身份证
     * @param record 同步过来的数据
     */
    public void processByDeleteA01(String a0184,Record record){
        Db.use(DBConstant.PG).tx(()-> {
            String syncId = StrKit.getRandomUUID().toUpperCase();
            if (StrUtil.isNotEmpty(a0184)) {
                //先删除sqlserver备份表使其同步
                int i = syncDao.deletePGDataSource("a01_fuling", "A0184", a0184);
                //备份和删除原表数据
                this.backDeleteByA01(a0184, syncId, "a01_fuling_delete");
                //记录日志表
                this.saveDeleteSync(syncId, a0184,"", record.getStr("a0101"), "1", "1", "删除" + record.getStr("A0101") + "的基本信息集", "a01");
            } else {
                this.saveDeleteSync(syncId, a0184,"", record.getStr("a0101"), "1", "0", "身份证号码为空", "");
            }
            return true;
        });
    }

    /**
     * 备份删除a01
     * @param a0184 身份证
     */
    private void backDeleteByA01(String a0184,String syncId,String tableName) {
        List<Record> pgDataSource = syncDao.findPgDataSource("a01", "A0184", a0184);
        if(ObjectUtil.isNotNull(pgDataSource) && pgDataSource.size() > 0){
            for (Record record : pgDataSource) {
                record.set("syncId",syncId);
            }
            //如果是不需要确认同步的话
            if(!isSure){
                syncDao.deletePGDataSource("a01","A0184",a0184);
            }
        }
        Db.use(DBConstant.PG).batchSave(tableName,pgDataSource,200);
    }


    /**
     * 身份证问题的报错细腻些
     * @param a0184 人员身份证
     * @param uuid 唯一标识
     * @param syncType 同步类型
     * @param syncFailureReason 失败的原因
     * @return
     */
    public void saveDeleteSync(String uuid,String a0184,String a0000,String a0101,String syncResult,String syncType,String syncFailureReason,String backTableName){
        Record syncArchivesResult = new Record();
        syncArchivesResult.set("id",uuid);
        syncArchivesResult.set("syncResult",syncResult);
        syncArchivesResult.set("A0000",a0000);
        syncArchivesResult.set("A0184",a0184);
        syncArchivesResult.set("A0101",a0101);
        syncArchivesResult.set("syncTime",new Date());
        syncArchivesResult.set("syncType",syncType);
        syncArchivesResult.set("syncFailureReason",syncFailureReason);
        syncArchivesResult.set("backTableName",backTableName);
        Db.use(DBConstant.PG).save("syncArchivesResult","id",syncArchivesResult);
    }


    /**
     * 获得字段对比映射
     * @param tableName 表名
     * @return key为sqlserver字段 value为
     */
    public Map<String,String> getComparedMap(String tableName){
        Map<String,String> result = new HashMap<>();
        if(StrUtil.equalsAny(tableName,"a01")){
            result.put("A0101,姓名","varchar");
            result.put("A0104,性别","varchar");
            result.put("A0107,出生年月","timestamp");
            result.put("A0111A,籍贯","varchar");
            result.put("A0114A,出生地","varchar");
            result.put("A0184,身份证","varchar");
            result.put("A0117,民族","varchar");
            result.put("A0134,参加工作时间","timestamp");
            result.put("A0128B,健康状态","varchar");
            result.put("A0155A,公务员登记号","varchar");
            result.put("A0187A,专长","varchar");
            result.put("A0160,人员类别","varchar");
            result.put("A0121,编制类型","varchar");
            result.put("A0221,职务层次","varchar");
            result.put("A0288,职务层次批准时间","varchar");
            result.put("A0192E,职级","varchar");
            result.put("A0196,专业职务综述","varchar");
        }
        if(StrUtil.equalsAny(tableName,"a05")){
            result.put("A0501B,职务层次(职级)等级","varchar");
            result.put("A0504,职务层次(职级)批准时间","timestamp");
        }
        if(StrUtil.equalsAny(tableName,"a06")){
            result.put("A0601,专业技术任职资格代码","varchar");
            result.put("A0602,专业技术任职资格名称","varchar");
            result.put("A0604,获得资格日期","timestamp");
            result.put("A0607,取得资格途径","varchar");
            result.put("A0611,评委会或考试名称","varchar");
        }
        if(StrUtil.equalsAny(tableName,"a08")){
            result.put("A0801B,学历代码","varchar");
            result.put("A0801A,学历名称","varchar");
            result.put("A0804,入学日期","timestamp");
            result.put("A0807,毕(肄)业日期","timestamp");
            result.put("A0824,所学专业名称","varchar");
            result.put("A0814,学校(单位)名称","varchar");
            result.put("A0827,所学专业类别","varchar");
            result.put("A0901A,学位名称","varchar");
            result.put("A0901B,学位代码","varchar");
            result.put("A0904,学位授予日期","timestamp");
            result.put("A0837,教育类别","varchar");
        }
        if(StrUtil.equalsAny(tableName,"a14")){
            result.put("A1404B,奖惩代码","varchar");
            result.put("A1404A,奖惩名称","varchar");
            result.put("A1407,奖惩批准日期","timestamp");
            result.put("A1411A,奖惩批准机关名称","varchar");
            result.put("A1414,批准奖惩机关级别","varchar");
            result.put("A1415,奖惩时职务层次","varchar");
            result.put("ISDISABLED,输出标识","varchar");
        }
        if(StrUtil.equalsAny(tableName,"a15")){
            result.put("A1517,考核结论","varchar");
            result.put("A1521,考核年度","timestamp");
        }
        if(StrUtil.equalsAny(tableName,"a36")){
            result.put("A3601,人员姓名","varchar");
            result.put("A3604A,人员称谓","varchar");
            result.put("A3607,出生年月","timestamp");
            result.put("A3611,工作单位及职务","varchar");
            result.put("A3627,政治面貌","varchar");
        }
        if(StrUtil.equals(tableName,"a44")){
            result.put("A1701,简历","varchar");
        }
        if(StrUtil.equals(tableName,"acq03")){
            result.put("A0140,政治面貌综述","varchar");
            result.put("A0141,政治面貌","varchar");
            result.put("A0144,入党时间","timestamp");
            result.put("A3921,第二党派","varchar");
            result.put("A3927,第三党派","varchar");
        }
        if(StrUtil.equals(tableName,"aphoto")){
            result.put("A0198,照片","varchar");
        }
        return result;
    }

}
