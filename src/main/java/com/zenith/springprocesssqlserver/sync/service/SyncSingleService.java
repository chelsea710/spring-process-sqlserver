package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 2021/4/9
 */
@Service
public class SyncSingleService {

    @Autowired
    private SyncDao syncDao;

    @Autowired
    private SyncA01Service syncA01Service;

//    @Autowired
//    private SyncA02Service syncA02Service;

    @Autowired
    private SyncA05Service syncA05Service;

    @Autowired
    private SyncA06Service syncA06Service;

    @Autowired
    private SyncA08Service syncA08Service;

    @Autowired
    private SyncA14Service syncA14Service;

    @Autowired
    private SyncA15Service syncA15Service;

    @Autowired
    private SyncA36Service syncA36Service;

    @Autowired
    private SyncA44Service syncA44Service;

    @Autowired
    private SyncAcq03Service syncAcq03Service;

    @Autowired
    private SyncAphoteService syncAphoteService;

    /**
     * 同步问题
     */
    public void sync(){
        //1:查询各个cdc表的长度
        List<Record> cdcTableNameList = syncDao.findCdcTableNameList();
        for (Record record : cdcTableNameList) {
            String cdcTableName = record.getStr("cdcTableName");
            String mappingTableName = record.getStr("mappingTableName");
            Integer cdcSystemTableCount = syncDao.findCdcSystemTableCount(cdcTableName);
            if(ObjectUtil.isNotNull(cdcSystemTableCount) && cdcSystemTableCount > 0){
                //2:如果有数据各自处理各自的东西
                this.processSyncTable(cdcTableName,mappingTableName);
            }
        }
    }

    /**
     * 根据数据和处理表的名字处理同步问题
     * @param cdcTableName cdc的表名
     * @param mappingTableName 需要处理的表名
     */
    private void processSyncTable(String cdcTableName, String mappingTableName) {
        if(StrUtil.equalsAny(mappingTableName,"a01")){
            syncA01Service.processCount(cdcTableName);
        }
        else {
            this.processCountUnlessA01(cdcTableName,mappingTableName);
        }
    }

    /**
     * 处理分表的数据
     * @param cdcTableName sqlserver表名
     * @param mappingTableName 对映的映射表名
     */
    public void processCountUnlessA01(String cdcTableName,String mappingTableName){
        //同步之后的删除数据
        List<Record> startLsnDeleteList = new ArrayList<>();
        List<Record> cdcTableList = syncDao.findCdcSystemTableList(cdcTableName);
        LinkedHashMap<Object, List<Record>> cdcTableMap = cdcTableList.stream().collect(Collectors.groupingBy(var -> cn.hutool.core.codec.Base64.encode(var.getBytes("__$update_mask")), LinkedHashMap::new, Collectors.toList()));
        for (Map.Entry<Object, List<Record>> entry : cdcTableMap.entrySet()) {
            if(entry.getValue().size() == 1){
                Record record = entry.getValue().get(0);
                String operation = record.getStr("__$operation");
                String leader_code = record.getStr("leader_code");
                String a0184 = syncDao.findA0184ByLeaderCode(leader_code);
                //删除数据?
                if(StrUtil.equalsAny(operation,"1")) {
                    if(StrUtil.equals(mappingTableName,"a05")) {
                        syncA05Service.processByDeleteA05(a0184, record);
                    }
                    if(StrUtil.equals(mappingTableName,"a06")) {
                        syncA06Service.processByDeleteA06(a0184, record);
                    }
                    if(StrUtil.equals(mappingTableName,"a08")){
                        syncA08Service.processByDeleteA08(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a14z1")){
                        syncA14Service.processByDeleteA14(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a15")){
                        syncA15Service.processByDeleteA15(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a36")){
                        syncA36Service.processByDeleteA36(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a44")){
                        syncA44Service.processByAddOrUpdateOrDeleteA44(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"acq03")){
                        syncAcq03Service.processByAddOrUpdateOrDeleteAcq03(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"aphoto")){
                        syncAphoteService.processByAddOrUpdateOrDeleteAphoto(a0184,record);
                    }
                }
                //新增数据
                if(StrUtil.equalsAny(operation,"2")){
                    if(StrUtil.equals(mappingTableName,"a05")) {
                        syncA05Service.processByAddA05(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a06")) {
                        syncA06Service.processByAddA06(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a08")){
                        syncA08Service.processByAddA08(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a14z1")){
                        syncA14Service.processByAddA14(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a15")){
                        syncA15Service.processByAddA15(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a36")){
                        syncA36Service.processByAddA36(a0184,record,record);
                    }
                    if(StrUtil.equals(mappingTableName,"a44")){
                        syncA44Service.processByAddOrUpdateOrDeleteA44(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"acq03")){
                        syncAcq03Service.processByAddOrUpdateOrDeleteAcq03(a0184,record);
                    }
                    if(StrUtil.equals(mappingTableName,"aphoto")){
                        syncAphoteService.processByAddOrUpdateOrDeleteAphoto(a0184,record);
                    }
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
                String leader_code = updateRecordAfter.getStr("leader_code");
                String a0184 = syncDao.findA0184ByLeaderCode(leader_code);
                if(StrUtil.equals(mappingTableName,"a05")) {
                    syncA05Service.processByAddA05(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a06")) {
                    syncA06Service.processByAddA06(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a08")){
                    syncA08Service.processByAddA08(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a14z1")){
                    syncA14Service.processByAddA14(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a15")){
                    syncA15Service.processByAddA15(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a36")){
                    syncA36Service.processByAddA36(a0184,updateRecordBefore,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"a44")){
                    syncA44Service.processByAddOrUpdateOrDeleteA44(a0184,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"acq03")){
                    syncAcq03Service.processByAddOrUpdateOrDeleteAcq03(a0184,updateRecordAfter);
                }
                if(StrUtil.equals(mappingTableName,"aphoto")){
                    syncAphoteService.processByAddOrUpdateOrDeleteAphoto(a0184,updateRecordAfter);
                }
            }
        }
        if(startLsnDeleteList.size() > 0){
            for (Record record : startLsnDeleteList) {
                Db.use(DBConstant.SQLSERVER).delete(cdcTableName,"__$start_lsn",record);
            }
        }
    }



}
