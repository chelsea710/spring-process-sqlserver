package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;

import com.zenith.springprocesssqlserver.config.Exce;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static com.zenith.springprocesssqlserver.constant.DBConstant.PG;


/**
 * @author LHR
 * @date 20210326
 */
@Service
public class SyncService {

    @Autowired
    private SyncDao syncDao;

    public void syncA01(){
        //涪陵区档案系统的a01的所有数据
        List<Record> sqlServerA01 = syncDao.findSqlServerA01();
        //人员简历信息
        LinkedHashMap<String, List<Record>> A1701Map = syncDao.findSqlServerA44().stream().collect(Collectors.groupingBy(var -> var.getStr("leader_code"), LinkedHashMap::new, Collectors.toList()));
        //人员政治面貌
        LinkedHashMap<String, List<Record>> Acq03Map = syncDao.findSqlServerAcq03().stream().collect(Collectors.groupingBy(var -> var.getStr("leader_code"), LinkedHashMap::new, Collectors.toList()));
        Map<String,String> zzmmMap = syncDao.getDicMap("GB4762");
        List<Record> savePgA01List = new ArrayList<>();

        for (Record record : sqlServerA01) {
            //A0000
            Record saveA01SingleRecord = new Record();
            this.setRecordA01(record,saveA01SingleRecord);
            //处理简历
            if(StrUtil.isNotEmpty(record.getStr("leader_code")) && A1701Map.containsKey(record.getStr("leader_code"))){
                StringBuilder a1701Result = new StringBuilder();
                List<Record> a1701List = A1701Map.get(record.getStr("leader_code"));
                for (Record a1701SingleRecord : a1701List) {
                    Date startdateTime = a1701SingleRecord.getDate("startdateTime");
                    if(ObjectUtil.isNotNull(startdateTime)){
                        a1701Result.append(DateUtil.format(startdateTime,"yyyy.MM")+"--");
                    }else {
                        a1701Result.append("       "+"--");
                    }
                    Date enddatetime = a1701SingleRecord.getDate("enddatetime");
                    if(ObjectUtil.isNotNull(enddatetime)){
                        a1701Result.append(DateUtil.format(enddatetime,"yyyy.MM")+"  ");
                    } else {
                        a1701Result.append("         ");
                    }
                    a1701Result.append(a1701SingleRecord.getStr("rzjg"));
                    a1701Result.append("\n");
                }
                saveA01SingleRecord.set("A1701",a1701Result.toString());
            }else {
                saveA01SingleRecord.set("A1701",null);
            }

            //政治面貌问题
            saveA01SingleRecord.set("A0140",null);
            saveA01SingleRecord.set("A0141",null);
            saveA01SingleRecord.set("A0144",null);
            saveA01SingleRecord.set("A3921",null);
            saveA01SingleRecord.set("A3927",null);
            if(Acq03Map.containsKey(record.getStr("leader_code"))){
                List<Record> records = Acq03Map.get(record.getStr("leader_code"));
                for (Record acq03Record : records) {
                    String acq0302 = acq03Record.getStr("acq0302");
                    if(StrUtil.equalsAny(acq0302,"01","02")){
                        saveA01SingleRecord.set("A0141",acq0302);
                        this.changeColByDate(acq03Record,saveA01SingleRecord,"acq0301","A0144");
                    } else if(StrUtil.equalsAny(acq0302,"01","02","03","12","13")){
                        saveA01SingleRecord.set("A0141",acq0302);
                        this.changeColByDate(acq03Record,saveA01SingleRecord,"acq0301","A0144");
                    } else {
                        if(StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A03921"))){
                            this.changeColByStr(acq03Record,saveA01SingleRecord,"acq0301","A3921");
                        } else if (StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A03927"))){
                            this.changeColByStr(acq03Record,saveA01SingleRecord,"acq0301","A3927");
                        }
                    }
                }
            }
            if(StrUtil.equalsAny(saveA01SingleRecord.getStr("A0141"),"01","02") && ObjectUtil.isNotNull(saveA01SingleRecord.getDate("A0144")) && StrUtil.isEmpty(saveA01SingleRecord.getStr("A3921")) && StrUtil.isEmpty(saveA01SingleRecord.getStr("A3927"))){
                saveA01SingleRecord.set("A0140",DateUtil.format(saveA01SingleRecord.getDate("A0144"),"yyyy.MM"));
            } else {
                List<String> joinList = new ArrayList<>();
                if(!StrUtil.equalsAny(saveA01SingleRecord.getStr("A0141"),"01","02") && zzmmMap.containsKey(saveA01SingleRecord.getStr("A0141"))){
                    joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A0141")));
                }

                if(StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A3921")) && zzmmMap.containsKey(saveA01SingleRecord.getStr("A3921"))){
                    joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A3921")));
                }

                if(StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A3927")) && zzmmMap.containsKey(saveA01SingleRecord.getStr("A3927"))){
                    joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A3927")));
                }
                String join = CollectionUtil.join(joinList, "、");
                if(ObjectUtil.isNotNull(record.getDate("A0144"))){
                    join+="("+DateUtil.format(record.getDate("A0144"),"yyyy.MM")+")";
                }
                saveA01SingleRecord.set("A0140",join);
            }

            //照片
            saveA01SingleRecord.set("A0198","/upload/impFile/Photos/"+record.getStr("leader_code")+".jpg");

            saveA01SingleRecord.set("A0123","9");
            savePgA01List.add(saveA01SingleRecord);
        }
        Db.use(PG).tx(()->{
            if(savePgA01List.size() > 0){
                Db.use(PG).batchSave("a01_fuling",savePgA01List,1000);
            }
            return true;
        });
    }

    /**
     * 处理政治面貌的数据
     * @param records sqlserver初始数据
     * @param saveA01SingleRecord sqlserver初始数据
     * @param a0000 人员唯一标识
     */
    public void setRecordAcq03(List<Record> records,Record saveA01SingleRecord,String a0000,Map<String,String> zzmmMap) {
        saveA01SingleRecord.set("A0000",a0000);
        saveA01SingleRecord.set("A0140", null);
        saveA01SingleRecord.set("A0141", null);
        saveA01SingleRecord.set("A0144", null);
        saveA01SingleRecord.set("A3921", null);
        saveA01SingleRecord.set("A3927", null);
        for (Record acq03Record : records) {
            String acq0302 = acq03Record.getStr("acq0302");
            if (StrUtil.equalsAny(acq0302, "01", "02")) {
                saveA01SingleRecord.set("A0141", acq0302);
                this.changeColByDate(acq03Record, saveA01SingleRecord, "acq0301", "A0144");
            } else if (StrUtil.equalsAny(acq0302, "01", "02", "03", "12", "13")) {
                saveA01SingleRecord.set("A0141", acq0302);
                this.changeColByDate(acq03Record, saveA01SingleRecord, "acq0301", "A0144");
            } else {
                if (StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A03921"))) {
                    this.changeColByStr(acq03Record, saveA01SingleRecord, "acq0301", "A3921");
                } else if (StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A03927"))) {
                    this.changeColByStr(acq03Record, saveA01SingleRecord, "acq0301", "A3927");
                }
            }
        }
        if (StrUtil.equalsAny(saveA01SingleRecord.getStr("A0141"), "01", "02") && ObjectUtil.isNotNull(saveA01SingleRecord.getDate("A0144")) && StrUtil.isEmpty(saveA01SingleRecord.getStr("A3921")) && StrUtil.isEmpty(saveA01SingleRecord.getStr("A3927"))) {
            saveA01SingleRecord.set("A0140", DateUtil.format(saveA01SingleRecord.getDate("A0144"), "yyyy.MM"));
        } else {
            List<String> joinList = new ArrayList<>();
            if (!StrUtil.equalsAny(saveA01SingleRecord.getStr("A0141"), "01", "02") && zzmmMap.containsKey(saveA01SingleRecord.getStr("A0141"))) {
                joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A0141")));
            }

            if (StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A3921")) && zzmmMap.containsKey(saveA01SingleRecord.getStr("A3921"))) {
                joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A3921")));
            }

            if (StrUtil.isNotEmpty(saveA01SingleRecord.getStr("A3927")) && zzmmMap.containsKey(saveA01SingleRecord.getStr("A3927"))) {
                joinList.add(zzmmMap.get(saveA01SingleRecord.getStr("A3927")));
            }
            String join = CollectionUtil.join(joinList, "、");
            if (ObjectUtil.isNotNull(saveA01SingleRecord.getDate("A0144"))) {
                join += "(" + DateUtil.format(saveA01SingleRecord.getDate("A0144"), "yyyy.MM") + ")";
            }
            saveA01SingleRecord.set("A0140", join);
        }
    }


    /**
     * 处理简历表的数据
     * @param a1701List sqlserver初始数据
     */
    public void setRecordA44(List<Record> a1701List,Record updateA01Record,String a0000){
        StringBuilder a1701Result = new StringBuilder();
        for (Record a1701SingleRecord : a1701List) {
            Date startdateTime = a1701SingleRecord.getDate("startdateTime");
            if(ObjectUtil.isNotNull(startdateTime)){
                a1701Result.append(DateUtil.format(startdateTime,"yyyy.MM")+"--");
            }else {
                a1701Result.append("       "+"--");
            }
            Date enddatetime = a1701SingleRecord.getDate("enddatetime");
            if(ObjectUtil.isNotNull(enddatetime)){
                a1701Result.append(DateUtil.format(enddatetime,"yyyy.MM")+"  ");
            } else {
                a1701Result.append("         ");
            }
            a1701Result.append(a1701SingleRecord.getStr("rzjg"));
            a1701Result.append("\n");
        }
        updateA01Record.set("A0000",a0000);
        updateA01Record.set("A1701",a1701Result.toString());
    }


    public void setRecordA01(Record sqlRecord,Record saveA01SingleRecord){
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"leader_code","A0000");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0101","A0101");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0104","A0104");
        this.changeColByDate(sqlRecord,saveA01SingleRecord,"a0107","A0107");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0111_1","A0111A");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0114_1","A0114A");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0184","A0184");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0117","A0117");
        this.changeColByDate(sqlRecord,saveA01SingleRecord,"a0134","A0134");

        //健康状态处理
        String a0127 = sqlRecord.getStr("a0127");
        if(StrUtil.equals(a0127,"10")){
            saveA01SingleRecord.set("A0128B","健康");
            saveA01SingleRecord.set("A0128","健康");
        } else if(StrUtil.equalsAny(a0127,"20")){
            saveA01SingleRecord.set("A0128B","一般");
            saveA01SingleRecord.set("A0128","一般");
        } else if(StrUtil.startWithAny(a0127,"3","4")){
            saveA01SingleRecord.set("A0128B","有慢性病");
            saveA01SingleRecord.set("A0128","有慢性病");
        } else if(StrUtil.startWithAny(a0127,"6","8")){
            saveA01SingleRecord.set("A0128B","残疾");
            saveA01SingleRecord.set("A0128","残疾");
        } else {
            saveA01SingleRecord.set("A0128B","其他");
            saveA01SingleRecord.set("A0128","其他");
        }

        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a01z8","A0155A");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0187_1","A0187A");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0160","A0160");
        this.changeColByStr(sqlRecord,saveA01SingleRecord,"a0165","A0165");
        //人员类别
        if(StrUtil.equalsAny(sqlRecord.getStr("A0160"),"7","8","B5")){
            saveA01SingleRecord.set("A0121","3");
        } else if (StrUtil.equalsAny(sqlRecord.getStr("A0160"),"A1")){
            saveA01SingleRecord.set("A0121","4");
        } else if (StrUtil.equalsAny(sqlRecord.getStr("A0160"),"A4","A5","A6")){
            saveA01SingleRecord.set("A0121","9");
        } else if (StrUtil.equalsAny(sqlRecord.getStr("A0160"),"1")){
            saveA01SingleRecord.set("A0121","1");
        } else if (StrUtil.equalsAny(sqlRecord.getStr("A0160"),"5")){
            saveA01SingleRecord.set("A0121","2");
        } else if (StrUtil.equalsAny(sqlRecord.getStr("A0160"),"6")){
            saveA01SingleRecord.set("A0121","3");
        } else {
            saveA01SingleRecord.set("A0121","9");
        }
    }


    public void syncA02(){
        //涪陵区档案系统的a02的所有数据
        List<Record> sqlServerA02 = syncDao.findSqlServerA02();
        List<Record> savePgA02List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();

        for (Record record : sqlServerA02) {
            Record saveA02SingleRecord = new Record();
            saveA02SingleRecord.set("A0200",StrKit.getRandomUUID().toUpperCase());
            saveA02SingleRecord.set("A0000",record.getStr("leader_code"));
            saveA02SingleRecord.set("A0201B",record.getStr("a0201_2"));
            saveA02SingleRecord.set("A0215A",record.getStr("a0215_1"));
            this.changeColByDate(record,saveA02SingleRecord,"a02z1_1","A0243");
            String a0279 = record.getStr("A0279");
            if(StrUtil.equals(a0279,"1")){
                saveA02SingleRecord.set("A0219","1");saveA02SingleRecord.set("A0201D","1");
            } else {
                saveA02SingleRecord.set("A0219","0");saveA02SingleRecord.set("A0201D","0");
            }
            this.changeColByInt(record,saveA02SingleRecord,"a0225","A0225");
            this.changeColByInt(record,saveA02SingleRecord,"a0223","A0223");
            this.changeColByStr(record,saveA02SingleRecord,"a02z4","A0215B");
            this.changeColByStr(record,saveA02SingleRecord,"a02zzzzz","mark");
            saveA02SingleRecord.set("A0255","1");

            String a02z2_2 = record.getStr("a02z2_2");
            if(StrUtil.isNotEmpty(a02z2_2)){
                Record updateA01SingleRecord = new Record();
                updateA01SingleRecord.set("A0000",record.getStr("leader_code"));
                updateA01SingleRecord.set("A0192",a02z2_2);
                updateA01SingleRecord.set("A0192A",a02z2_2);
                updatePgA01List.add(updateA01SingleRecord);
            }
            saveA02SingleRecord.set("A0281","1");
            savePgA02List.add(saveA02SingleRecord);
        }

        Db.use(PG).tx(()->{
            if(savePgA02List.size() > 0){
                Db.use(PG).batchSave("a02_fuling",savePgA02List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }

    public void setRecordA02(Record record, Record saveA02SingleRecord,Record updateA01SingleRecord, String a0000, Map<String,String> gb8561) {
        saveA02SingleRecord.set("A0200",StrKit.getRandomUUID().toUpperCase());
        saveA02SingleRecord.set("A0000",a0000);
        saveA02SingleRecord.set("A0201B",record.getStr("a0201_2"));
        saveA02SingleRecord.set("A0215A",record.getStr("a0215_1"));
        this.changeColByDate(record,saveA02SingleRecord,"a02z1_1","A0243");
        String a0279 = record.getStr("A0279");
        if(StrUtil.equals(a0279,"1")){
            saveA02SingleRecord.set("A0219","1");saveA02SingleRecord.set("A0201D","1");
        } else {
            saveA02SingleRecord.set("A0219","0");saveA02SingleRecord.set("A0201D","0");
        }
        this.changeColByInt(record,saveA02SingleRecord,"a0225","A0225");
        this.changeColByInt(record,saveA02SingleRecord,"a0223","A0223");
        this.changeColByStr(record,saveA02SingleRecord,"a02z4","A0215B");
        this.changeColByStr(record,saveA02SingleRecord,"a02zzzzz","mark");
        saveA02SingleRecord.set("A0255","1");

        String a02z2_2 = record.getStr("a02z2_2");
        if(StrUtil.isNotEmpty(a02z2_2)){
            updateA01SingleRecord.set("A0000",record.getStr("leader_code"));
            updateA01SingleRecord.set("A0192",a02z2_2);
            updateA01SingleRecord.set("A0192A",a02z2_2);
        }
        saveA02SingleRecord.set("A0281","1");
    }


    public void syncA05(){
        List<Record> sqlServerA05 = syncDao.findSqlServerA05();
        List<Record> savePgA05List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();
        Map<String, String> zxccMap = this.zwccMap();
        Map<String, String> zwzjMap = this.zwzjMap();

        for (Record record : sqlServerA05) {
            String a0501_2 = record.getStr("a0501_2");
            if(StrUtil.isNotEmpty(a0501_2)) {
                Record saveA05SingleRecord = new Record();
                saveA05SingleRecord.set("A0500", StrKit.getRandomUUID().toUpperCase());
                this.changeColByStr(record, saveA05SingleRecord, "leader_code", "A0000");
                this.changeColByDate(record, saveA05SingleRecord, "a0504", "A0504");
                if(zxccMap.containsKey(a0501_2)){
                    saveA05SingleRecord.set("A0531","0");
                    saveA05SingleRecord.set("A0524","1");
                    saveA05SingleRecord.set("A0525","1");
                    saveA05SingleRecord.set("A0501B",zxccMap.get(a0501_2));
                    Record updateA01Update = new Record();
                    updateA01Update.set("A0000",record.getStr("leader_code"));
                    updateA01Update.set("A0288",record.getDate("a0504"));
                    updateA01Update.set("A0221",zxccMap.get(a0501_2));
                    updateA01Update.set("A0192E",null);
                    updatePgA01List.add(updateA01Update);
                } else if(zwzjMap.containsKey(a0501_2)){
                    saveA05SingleRecord.set("A0531","1");
                    saveA05SingleRecord.set("A0524","1");
                    saveA05SingleRecord.set("A0525","1");
                    saveA05SingleRecord.set("A0501B",zxccMap.get(a0501_2));
                    Record updateA01Update = new Record();
                    updateA01Update.set("A0000",record.getStr("leader_code"));
                    updateA01Update.set("A0288",null);
                    updateA01Update.set("A0221",null);
                    updateA01Update.set("A0192E",zxccMap.get(a0501_2));
                    updatePgA01List.add(updateA01Update);
                } else {
                    saveA05SingleRecord.set("A0531",null);
                    saveA05SingleRecord.set("A0524",null);
                    saveA05SingleRecord.set("A0525",null);
                    saveA05SingleRecord.set("A0501B",null);
                }
                savePgA05List.add(saveA05SingleRecord);
            }
        }


        Db.use(PG).tx(()->{
            if(savePgA05List.size() > 0){
                Db.use(PG).batchSave("a05_fuling",savePgA05List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }


    public void setRecordA05(Record record,Record saveA05SingleRecord,String A0000,Map<String, String> zxccMap,Map<String, String> zwzjMap){
        String a0501_2 = record.getStr("a0501_2");
        if(StrUtil.isNotEmpty(a0501_2)) {
            saveA05SingleRecord.set("A0500", StrKit.getRandomUUID().toUpperCase());
            saveA05SingleRecord.set("A0000",A0000);
            this.changeColByDate(record, saveA05SingleRecord, "a0504", "A0504");
            if(zxccMap.containsKey(a0501_2)){
                saveA05SingleRecord.set("A0531","0");
                saveA05SingleRecord.set("A0524","1");
                saveA05SingleRecord.set("A0525","1");
                saveA05SingleRecord.set("A0501B",zxccMap.get(a0501_2));
                Record updateA01Update = new Record();
                updateA01Update.set("A0000",A0000);
                updateA01Update.set("A0288",record.getDate("a0504"));
                updateA01Update.set("A0221",zxccMap.get(a0501_2));
                updateA01Update.set("A0192E",null);
            } else if(zwzjMap.containsKey(a0501_2)){
                saveA05SingleRecord.set("A0531","1");
                saveA05SingleRecord.set("A0524","1");
                saveA05SingleRecord.set("A0525","1");
                saveA05SingleRecord.set("A0501B",zxccMap.get(a0501_2));
                Record updateA01Update = new Record();
                updateA01Update.set("A0000",A0000);
                updateA01Update.set("A0288",null);
                updateA01Update.set("A0221",null);
                updateA01Update.set("A0192E",zxccMap.get(a0501_2));
            } else {
                saveA05SingleRecord.set("A0531",null);
                saveA05SingleRecord.set("A0524",null);
                saveA05SingleRecord.set("A0525",null);
                saveA05SingleRecord.set("A0501B",null);
            }
        }
    }



    public Map<String,String> zwccMap(){
        Map<String,String> result = new HashMap<>();
        result.put("0101","1A01");
        result.put("0102","1A02");
        result.put("0111","1A11");
        result.put("0112","1A12");
        result.put("0121","1A21");
        result.put("0122","1A22");
        result.put("0131","1A31");
        result.put("0132","1A32");
        result.put("0141","1A41");
        result.put("0142","1A42");
        result.put("0150","1A50");
        result.put("0160","1A60");
        result.put("0198","1A98");
        result.put("0199","1A99");


        result.put("0211","901");
        result.put("0212","902");
        result.put("0221","903");
        result.put("0222","904");
        result.put("0231","905");
        result.put("0232","906");
        result.put("0241","907");
        result.put("0242","908");
        result.put("0250","909");
        result.put("0260","910");
        result.put("0298","998");
        result.put("0299","999");


        result.put("0311","C01");
        result.put("0312","C02");
        result.put("0313","C03");
        result.put("0314","C04");
        result.put("0321","C05");
        result.put("0322","C06");
        result.put("0323","C07");
        result.put("0331","C08");
        result.put("0332","C09");
        result.put("0333","C10");
        result.put("0341","C11");
        result.put("0342","C12");
        result.put("0351","C13");
        result.put("0399","C99");

        result.put("0410","D01");
        result.put("0420","D02");
        result.put("0430","D03");
        result.put("0440","D04");
        result.put("0450","D05");
        result.put("0499","D09");

        result.put("0510","E01");
        result.put("0599","E09");

        result.put("0610","F01");
        result.put("0620","F02");
        result.put("0630","F03");
        result.put("0640","F04");
        result.put("0650","F05");
        result.put("0699","F09");

        result.put("0710","G01");
        result.put("0799","G09");




        return result;
    }

    public Map<String,String> zwzjMap(){
        Map<String,String> result = new HashMap<>();
        result.put("0171","11");
        result.put("0172","12");
        result.put("0173","13");
        result.put("0174","14");
        result.put("0175","15");
        result.put("0176","16");
        result.put("0177","17");
        result.put("0178","18");
        result.put("0179","19");
        result.put("0180","1A");
        result.put("0181","1B");
        result.put("0182","1C");


        result.put("080","67");
        result.put("081","68");
        result.put("082","69");
        result.put("083","6A");
        result.put("084","6B");
        result.put("085","6C");
        result.put("086","17");

        return result;
    }


    public void syncA06(){
        List<Record> sqlServerA06 = syncDao.findSqlServerA06();
        List<Record> savePgA06List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();
        Map<String, String> gb8561 = syncDao.getDicMap("GB8561");

        for (Record record : sqlServerA06) {
            Record saveA06Single = new Record();
            saveA06Single.set("A0600",StrKit.getRandomUUID().toUpperCase());
            this.changeColByStr(record,saveA06Single,"a0601","A0601");
            this.changeColByStr(record,saveA06Single,"leader_code","A0000");
            saveA06Single.set("A0602",gb8561.getOrDefault(record.getStr("a0601"),""));
            this.changeColByDate(record,saveA06Single,"a0604","A0604");
            this.changeColByStr(record,saveA06Single,"a0607","A0607");
            this.changeColByStr(record,saveA06Single,"a0611","A0611");
            this.changeColByStr(record,saveA06Single,"a06z1","A0614");
            this.changeColByStr(record,saveA06Single,"del","A0699");
            savePgA06List.add(saveA06Single);
        }


        Db.use(PG).tx(()->{
            if(savePgA06List.size() > 0){
                Db.use(PG).batchSave("a06_fuling",savePgA06List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }

    /**
     * 设置a06的值
     * @param record sqlserver的数据
     * @param saveA06Single pg需要的数据
     */
    public Record setRecordA06(Record record,Record saveA06Single,String a0000,Map<String, String> gb8561){
        saveA06Single.set("A0600",StrKit.getRandomUUID().toUpperCase());
        this.changeColByStr(record,saveA06Single,"a0601","A0601");
        saveA06Single.set("A0000",a0000);
        saveA06Single.set("A0602",gb8561.getOrDefault(record.getStr("a0601"),""));
        this.changeColByDate(record,saveA06Single,"a0604","A0604");
        this.changeColByStr(record,saveA06Single,"a0607","A0607");
        this.changeColByStr(record,saveA06Single,"a0611","A0611");
        this.changeColByStr(record,saveA06Single,"a06z1","A0614");
        this.changeColByStr(record,saveA06Single,"del","A0699");
        return saveA06Single;
    }

    public void syncA08(){
        List<Record> sqlServerA08 = syncDao.findSqlServerA08();
        Set<String> zb64Set = syncDao.getDic("ZB64");
        Map<String, String> zb64 = syncDao.getDicMap("ZB64");
        List<Record> savePgA08List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();

        for (Record record : sqlServerA08) {
            Record saveA08Single = new Record();
            saveA08Single.set("A0800",StrKit.getRandomUUID().toUpperCase());
            this.changeColByStr(record,saveA08Single,"leader_code","A0000");
            //处理学历代码
            String a0801_2 = record.getStr("a0801_2");
            if(StrUtil.startWithAny(a0801_2,"1")){
                this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","11");
            } else if(StrUtil.startWithAny(a0801_2,"2")){
                this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","21");
            } else if(StrUtil.startWithAny(a0801_2,"3")){
                this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","31");
            } else {
                this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","");
            }
            saveA08Single.set("A0801A",zb64.getOrDefault(saveA08Single.getStr("A0801B"),""));
            this.changeColByDate(record,saveA08Single,"a0804","A0804");
            this.changeColByDate(record,saveA08Single,"a0807","A0807");
            this.changeColByStr(record,saveA08Single,"a0824","A0824");
            this.changeColByStr(record,saveA08Single,"a0814","A0814");
            String a08z4 = record.getStr("a08z4");
            if(StrUtil.isNotEmpty(a08z4) && a08z4.length() > 1){
                saveA08Single.set("A0827",a08z4.substring(1));
            }else {
                saveA08Single.set("A0827","");
            }
            this.changeColByStr(record,saveA08Single,"a0915","A0901A");
            this.changeColByStr(record,saveA08Single,"a0901_2","A0901B");
            this.changeColByDate(record,saveA08Single,"a0904","A0904");
            String a0837 = record.getStr("a0837");
            if(StrUtil.equals(a0837,"1")){
                saveA08Single.set("A0837","1");
            } else if (StrUtil.equals(a0837,"2")){
                saveA08Single.set("A0837","0");
            }else {
                saveA08Single.set("A0837",null);
            }
            if(!StrUtil.equals(record.getStr("a0834"),"0") && StrUtil.isNotEmpty(record.getStr("a0834"))) {
                saveA08Single.set("A0898", "1");
            } else {
                saveA08Single.set("A0898",null);
            }
            savePgA08List.add(saveA08Single);
        }


        Db.use(PG).tx(()->{
            if(savePgA08List.size() > 0){
                Db.use(PG).batchSave("a08_fuling",savePgA08List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }

            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0834\"='0',\"A0831\"='0',\"A0832\"='0',\"A0835\"='0',\"A0838\"='0',\"A0839\"='0'");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0831\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0801B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0837\"='1' and \"A0801B\" is not null and \"A0801B\" <> '') s WHERE s.\"st\" = 1 )");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0832\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0901B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0837\"='1' and \"A0901B\" is not null and \"A0901B\" <> '') s WHERE s.\"st\" = 1 )");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0834\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0801B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0801B\" is not null and \"A0801B\" <> '') s WHERE s.\"st\" = 1 )");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0835\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0901B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0901B\" is not null and \"A0901B\" <> '') s WHERE s.\"st\" = 1 )");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0838\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0801B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0837\"='0' and \"A0801B\" is not null and \"A0801B\" <> '') s WHERE s.\"st\" = 1 )");
            Db.use(PG).update("UPDATE \"a08_fuling\" SET \"A0839\" = '1' WHERE \"A0800\" IN (SELECT s.\"A0800\" FROM ( SELECT \"A0800\",ROW_NUMBER () OVER ( PARTITION BY \"A0000\" ORDER BY \"A0901B\" ) AS \"st\" FROM \"a08_fuling\" where \"A0837\"='0' and \"A0901B\" is not null and \"A0901B\" <> '') s WHERE s.\"st\" = 1 )");

            return true;
        });
    }

    /**
     * 设置a06的值
     * @param record sqlserver的数据
     * @param saveA08Single pg需要的数据
     */
    public Record setRecordA08(Record record,Record saveA08Single,String a0000,Set<String> zb64Set,Map<String, String> zb64){
        saveA08Single.set("A0800",StrKit.getRandomUUID().toUpperCase());
        saveA08Single.set("A0000",a0000);
        //处理学历代码
        String a0801_2 = record.getStr("a0801_2");
        if(StrUtil.startWithAny(a0801_2,"1")){
            this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","11");
        } else if(StrUtil.startWithAny(a0801_2,"2")){
            this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","21");
        } else if(StrUtil.startWithAny(a0801_2,"3")){
            this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","31");
        } else {
            this.processDic(zb64Set,saveA08Single,a0801_2,"A0801B","");
        }
        saveA08Single.set("A0801A",zb64.getOrDefault(saveA08Single.getStr("A0801B"),""));
        this.changeColByDate(record,saveA08Single,"a0804","A0804");
        this.changeColByDate(record,saveA08Single,"a0807","A0807");
        this.changeColByStr(record,saveA08Single,"a0824","A0824");
        this.changeColByStr(record,saveA08Single,"a0814","A0814");
        String a08z4 = record.getStr("a08z4");
        if(StrUtil.isNotEmpty(a08z4) && a08z4.length() > 1){
            saveA08Single.set("A0827",a08z4.substring(1));
        }else {
            saveA08Single.set("A0827","");
        }
        this.changeColByStr(record,saveA08Single,"a0915","A0901A");
        this.changeColByStr(record,saveA08Single,"a0901_2","A0901B");
        this.changeColByDate(record,saveA08Single,"a0904","A0904");
        String a0837 = record.getStr("a0837");
        if(StrUtil.equals(a0837,"1")){
            saveA08Single.set("A0837","1");
        } else if (StrUtil.equals(a0837,"2")){
            saveA08Single.set("A0837","0");
        }else {
            saveA08Single.set("A0837",null);
        }
        if(!StrUtil.equals(record.getStr("a0834"),"0") && StrUtil.isNotEmpty(record.getStr("a0834"))) {
            saveA08Single.set("A0898", "1");
        } else {
            saveA08Single.set("A0898",null);
        }
        return saveA08Single;
    }


    public void syncA14(){
        List<Record> sqlServerA14z2 = syncDao.findSqlServerA14z2();
        List<Record> sqlServerA14z3 = syncDao.findSqlServerA14z3();
        List<Record> savePgA14List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();
        List<Record> savePgA14z3List = new ArrayList<>();
        Map<String, String> map = this.zwccMap();

        for (Record record : sqlServerA14z2) {
            Record saveA14Single = new Record();
            saveA14Single.set("A1400",StrKit.getRandomUUID().toUpperCase());
            this.changeColByStr(record,saveA14Single,"leader_code","A0000");
            //奖惩类型
            String a14z201 = record.getStr("a14z201");
            if(StrUtil.startWithAny(a14z201,"02") && !StrUtil.equals(a14z201,"021154")){
                saveA14Single.set("A1404B","01115");
            } else if (StrUtil.startWithAny(a14z201,"03") || StrUtil.equals(a14z201,"021154")){
                saveA14Single.set("A1404B","01119");
            } else {
                saveA14Single.set("A1404B",a14z201);
            }
            this.changeColByStr(record,saveA14Single,"a14z204_1","A1404A");
            this.changeColByDate(record,saveA14Single,"a14z211","A1407");
            this.changeColByStr(record,saveA14Single,"a14z214_1","A1411A");
            this.changeColByStr(record,saveA14Single,"a14z217","A1414");
            saveA14Single.set("A1415",map.getOrDefault(record.getStr("a14z315"),""));

            String a14z2z2 = record.getStr("a14z2z2");
            if(StrUtil.equalsAny(a14z2z2,"0")){
                saveA14Single.set("ISDISABLED",1);
            }else {
                saveA14Single.set("ISDISABLED",null);

            }
            savePgA14List.add(saveA14Single);
        }


        Db.use(PG).tx(()->{
            if(savePgA14List.size() > 0){
                Db.use(PG).batchSave("a14_fuling",savePgA14List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });


        for (Record a14z3Eecord : sqlServerA14z3) {
            Record saveA14Single = new Record();
            saveA14Single.set("A1400",StrKit.getRandomUUID().toUpperCase());
            this.changeColByStr(a14z3Eecord,saveA14Single,"leader_code","A0000");
            //奖惩类型
            this.changeColByStr(a14z3Eecord,saveA14Single,"a14z301","A1404B");
            this.changeColByStr(a14z3Eecord,saveA14Single,"a14z304_1","A1404A");
            this.changeColByDate(a14z3Eecord,saveA14Single,"a14z311","A1407");
            this.changeColByStr(a14z3Eecord,saveA14Single,"a14z314_1","A1411A");
            this.changeColByStr(a14z3Eecord,saveA14Single,"a14z317","A1414");
            saveA14Single.set("A1415",map.getOrDefault(a14z3Eecord.getStr("a14z315"),""));
            this.changeColByDate(a14z3Eecord,saveA14Single,"a14z324","A1424");
            saveA14Single.set("ISDISABLED",null);
            savePgA14z3List.add(saveA14Single);
        }


        Db.use(PG).tx(()->{
            if(savePgA14List.size() > 0){
                Db.use(PG).batchSave("a14_fuling",savePgA14z3List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }


    public void setRecordA14(Record record,Record saveA14Single,String a0000,Map<String, String> map){
        saveA14Single.set("A1400",StrKit.getRandomUUID().toUpperCase());
        saveA14Single.set("A0000",a0000);
        //奖惩类型
        String a14z201 = record.getStr("a14z201");
        if(StrUtil.startWithAny(a14z201,"02") && !StrUtil.equals(a14z201,"021154")){
            saveA14Single.set("A1404B","01115");
        } else if (StrUtil.startWithAny(a14z201,"03") || StrUtil.equals(a14z201,"021154")){
            saveA14Single.set("A1404B","01119");
        } else {
            saveA14Single.set("A1404B",a14z201);
        }
        this.changeColByStr(record,saveA14Single,"a14z204_1","A1404A");
        this.changeColByDate(record,saveA14Single,"a14z211","A1407");
        this.changeColByStr(record,saveA14Single,"a14z214_1","A1411A");
        this.changeColByStr(record,saveA14Single,"a14z217","A1414");
        saveA14Single.set("A1415",map.getOrDefault(record.getStr("a14z315"),""));

        String a14z2z2 = record.getStr("a14z2z2");
        if(StrUtil.equalsAny(a14z2z2,"0")){
            saveA14Single.set("ISDISABLED",1);
        }else {
            saveA14Single.set("ISDISABLED",null);

        }
    }



    public Map<String,String> getA1404BMap(){
        Map<String,String> result = new HashMap<>();
        return result;
    }


    public void syncA15(){
        List<Record> sqlServerA15 = syncDao.findSqlServerAcq01();
        Map<String, String> map = this.a1517Map();
        List<Record> savePgA15List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();

        for (Record record : sqlServerA15) {

            Integer acp0101 = record.getInt("acq0101");
            if(ObjectUtil.isNotNull(acp0101) && acp0101>=1000){
                Record a15SingleRecord = new Record();
                a15SingleRecord.set("A1500",StrKit.getRandomUUID().toUpperCase());
                this.changeColByStr(record,a15SingleRecord,"leader_code","A0000");
                a15SingleRecord.set("A1521",DateUtil.parse(String.valueOf(acp0101),"yyyy"));
                String acq0161 = record.getStr("acq0161");
                if(map.containsKey(acq0161)){
                    a15SingleRecord.set("A1517",map.get(acq0161));
                } else {
                    a15SingleRecord.set("A1517",acq0161);
                }
                savePgA15List.add(a15SingleRecord);
            }
        }

        Db.use(PG).tx(()->{
            if(savePgA15List.size() > 0){
                Db.use(PG).batchSave("a15_fuling",savePgA15List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }

    public void setRecordA15(Record record,Record a15SingleRecord,String a0000,Map<String, String> map){
        Integer acp0101 = record.getInt("acq0101");
        if(ObjectUtil.isNotNull(acp0101) && acp0101>=1000){
            a15SingleRecord.set("A1500",StrKit.getRandomUUID().toUpperCase());
            a15SingleRecord.set("A0000",a0000);
            a15SingleRecord.set("A1521",DateUtil.parse(String.valueOf(acp0101),"yyyy"));
            String acq0161 = record.getStr("acq0161");
            if(map.containsKey(acq0161)){
                a15SingleRecord.set("A1517",map.get(acq0161));
            } else {
                a15SingleRecord.set("A1517",acq0161);
            }
        }
    }


    public Map<String,String> a1517Map(){
        Map<String,String> result = new HashMap<>();
        result.put("70","6B");result.put("71","2B");
        result.put("75","3B");result.put("78","4B");
        result.put("95","6B");
        result.put("99","6B");
        result.put("9A","6B");
        return result;
    }


    public void syncA36(){
        List<Record> sqlServerA36 = syncDao.findSqlServerA36();
        List<Record> savePgA36List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();
        Map<String, String> a3604Map = this.getA3604Map();

        for (Record record : sqlServerA36) {
            Record a36SingleRecord = new Record();
            this.changeColByStr(record,a36SingleRecord,"id","A3600");
            this.changeColByStr(record,a36SingleRecord,"leader_code","A0000");
            this.changeColByInt(record,a36SingleRecord,"a3647","SORTID");
            String a3604_1 = record.getStr("a3604_1");
            //太多不好整

            a36SingleRecord.set("A3604A",a3604Map.getOrDefault(a3604_1,""));
            this.changeColByStr(record,a36SingleRecord,"a3601","A3601");
            this.changeColByDate(record,a36SingleRecord,"a3607","A3607");
            this.changeColByStr(record,a36SingleRecord,"a3611","A3611");
            this.changeColByStr(record,a36SingleRecord,"a3627","A3627");
            a36SingleRecord.set("A3699",1);
            savePgA36List.add(a36SingleRecord);
        }

        Db.use(PG).tx(()->{
            if(savePgA36List.size() > 0){
                Db.use(PG).batchSave("a36_fuling",savePgA36List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }


    public void setRecordA36(Record record, Record a36SingleRecord, String a0000, Map<String,String> a3604Map) {
        this.changeColByStr(record,a36SingleRecord,"id","A3600");
        a36SingleRecord.set("A0000",a0000);
        this.changeColByInt(record,a36SingleRecord,"a3647","SORTID");
        String a3604_1 = record.getStr("a3604_1");
        a36SingleRecord.set("A3604A",a3604Map.getOrDefault(a3604_1,""));
        this.changeColByStr(record,a36SingleRecord,"a3601","A3601");
        this.changeColByDate(record,a36SingleRecord,"a3607","A3607");
        this.changeColByStr(record,a36SingleRecord,"a3611","A3611");
        this.changeColByStr(record,a36SingleRecord,"a3627","A3627");
        a36SingleRecord.set("A3699",1);
    }

    public Map<String,String> getA3604Map(){
        Map<String,String> result = new HashMap<>();
        result.put("11","丈夫");
        result.put("12","妻子");
        result.put("21","儿子");
        result.put("22","长子");
        result.put("23","次子");
        result.put("24","三子");
        result.put("25","四子");
        result.put("26","五子");
        result.put("27","养子");
        result.put("28","女婿");
        result.put("29","其他子");

        result.put("31","女儿");
        result.put("32","长女");
        result.put("33","次女");
        result.put("34","三女");
        result.put("35","四女");
        result.put("36","五女");
        result.put("37","养女");
        result.put("38","儿媳");
        result.put("39","其他女儿");

        result.put("41","孙子");
        result.put("43","孙女");
        result.put("44","外孙子");
        result.put("45","孙媳妇");
        result.put("46","孙女婿");
        result.put("47","曾孙子");
        result.put("48","曾孙女");
        result.put("49","其他孙子");


        result.put("51","父亲");
        result.put("52","母亲");
        result.put("53","公公");
        result.put("54","婆婆");
        result.put("55","岳父");
        result.put("56","岳母");
        result.put("57","继父");
        result.put("58","继母");
        result.put("59","养父");
        result.put("5a","养母");
        result.put("5b","其他父母关系");


        result.put("71","哥哥");
        result.put("72","嫂子");
        result.put("73","弟弟");
        result.put("74","弟媳");
        result.put("75","姐姐");
        result.put("76","姐夫");
        result.put("77","妹妹");
        result.put("78","妹夫");
        result.put("7A","夫兄");
        result.put("7B","夫弟");
        result.put("7C","夫姐");
        result.put("7D","夫妹");
        result.put("7E","妻兄");
        result.put("7F","妻弟");
        result.put("7G","妻姐");
        result.put("7H","妻妹");

        result.put("81","伯父");
        result.put("82","伯母");
        result.put("83","叔父");
        result.put("84","婶母");
        result.put("85","舅父");
        result.put("86","舅母");
        result.put("87","姨夫");
        result.put("88","姨母");
        result.put("89","姑父");
        result.put("8A","姑母");
        result.put("8B","堂兄弟");
        result.put("8C","表兄弟");
        result.put("8D","侄子");
        result.put("8E","侄女");
        result.put("8F","外甥女");
        result.put("8G","其他亲属");
        result.put("8H","保姆");
        result.put("8I","非亲属");
        result.put("8J","其他");

        return result;

    }

    public void syncB01(){
        List<Record> sqlServerB01 = syncDao.findSqlServerB01();
        List<Record> savePgB01List = new ArrayList<>();
        List<Record> updatePgA01List = new ArrayList<>();

        for (Record record : sqlServerB01) {
            Record b01SingleRecord = new Record();
            this.changeColByStr(record,b01SingleRecord,"dw_code","id");
            this.changeColByStr(record,b01SingleRecord,"b0101","B0101");
            this.changeColByStr(record,b01SingleRecord,"b0104","B0104");
            this.changeColByStr(record,b01SingleRecord,"b0114","B0114");
            savePgB01List.add(b01SingleRecord);
        }

        Db.use(PG).tx(()->{
            if(savePgB01List.size() > 0){
                Db.use(PG).batchSave("b01_fuling",savePgB01List,1000);
            }
            if(updatePgA01List.size() > 0){
                Db.use(PG).batchUpdate("a01_fuling","A0000",updatePgA01List,1000);
            }
            return true;
        });
    }



    public void processDic(Set<String> dicSet,Record saveReocrd,String sqlserverRe,String pgCol,String pgColRE){
        if(dicSet.contains(sqlserverRe)){
            saveReocrd.set(pgCol,sqlserverRe);
        } else {
            saveReocrd.set(pgCol,pgColRE);
        }
    }

    public void syncphotos() throws Exception{
        Integer pageNum = 1;
        while (true) {
            Page<Record> sqlServerPhoto = syncDao.findSqlServerPhoto(pageNum,1000);
            if(sqlServerPhoto.getList().size()!=0) {
                for (Record record : sqlServerPhoto.getList()) {
                    byte[] aphoto01s = record.getBytes("aphoto01");
                    if (ObjectUtil.isNotNull(aphoto01s) && aphoto01s.length > 0) {
                        String leader_code = record.getStr("leader_code");
                        ByteArrayInputStream byteArrayInputStream = IoUtil.toStream(aphoto01s);
                        File file = new File("/home/photos/" + leader_code + ".jpg");
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        IoUtil.copy(byteArrayInputStream, fileOutputStream);
                        byteArrayInputStream.close();
                        fileOutputStream.close();
                    }
                }
                pageNum++;
            }else {
                break;
            }
        }
    }


    public void changeColByDate(Record sqlserverRecord,Record pgRecord,String sqlserver,String pg){
        Date col = sqlserverRecord.getDate(sqlserver);
        pgRecord.set(pg,col);
    }

    public void changeColByStr(Record sqlserverRecord,Record pgRecord,String sqlserver,String pg){
        String col = sqlserverRecord.getStr(sqlserver);
        pgRecord.set(pg,col);
    }

    public void changeColByInt(Record sqlserverRecord,Record pgRecord,String sqlserver,String pg){
        Integer col = sqlserverRecord.getInt(sqlserver);
        pgRecord.set(pg,col);
    }

    /**
     *
     * @return 找寻需要的照片 会放在 /home/findpic下
     */
    public String findPic() throws Exception {
        Set<String> strings = syncDao.findfindPicTable();
        for (String a0000 : strings) {
            if(FileUtil.exist("/home/photos/"+a0000+".jpg")){
                FileOutputStream fileOutputStream = new FileOutputStream(FileUtil.newFile("/home/gb/"+a0000+".jpg"));
                FileInputStream fileInputStream = IoUtil.toStream(new File("/home/photos/"+a0000+".jpg"));
                IoUtil.copy(fileInputStream,fileOutputStream);
                fileInputStream.close();
                fileOutputStream.close();
            }
        }

        return "执行完成";
    }


    public void processPng() throws Exception{
        Set<String> a01Records = syncDao.findPgCityAnfCompanyMem();
        List<Record> saveA01Record = new ArrayList<>();
        Integer pageNum = 1;
        while (true) {
            Page<Record> sqlServerPhoto = syncDao.findSqlServerPhoto(pageNum,1000);
            if(sqlServerPhoto.getList().size()!=0) {
                for (Record record : sqlServerPhoto.getList()) {
                    byte[] aphoto01s = record.getBytes("aphoto01");
                    if (ObjectUtil.isNotNull(aphoto01s) && aphoto01s.length > 0) {
                        String fileType = this.checkType(this.bytesToHexString(aphoto01s));
                        String leader_code = record.getStr("leader_code");
                        if(!StrUtil.equalsAny(fileType,".jpg","0000") && a01Records.contains(leader_code)) {
                            ByteArrayInputStream byteArrayInputStream = IoUtil.toStream(aphoto01s);
                            File file = new File("/home/photos/" + leader_code +fileType);
                            FileOutputStream fileOutputStream = new FileOutputStream(file);
                            IoUtil.copy(byteArrayInputStream, fileOutputStream);
                            byteArrayInputStream.close();
                            fileOutputStream.close();

                            Record a01 = new Record();
                            a01.set("A0000",leader_code);
                            a01.set("A0198","/upload/impFile/Photos/"+leader_code + fileType);
                            saveA01Record.add(a01);
                        }
                    }
                }
                pageNum++;
            }else {
                break;
            }
        }

        if(saveA01Record.size() > 0){
            Db.use(PG).tx(()->{
                Db.use(PG).batchSave("fulingPic",saveA01Record,1000);
                return true;
            });
        }
    }


    public void setRecordAphoto(Record record,Record saveA01Record) {
        try {
            byte[] aphoto01s = record.getBytes("aphoto01");
            if (ObjectUtil.isNotNull(aphoto01s) && aphoto01s.length > 0) {
                String fileType = this.checkType(this.bytesToHexString(aphoto01s));
                String leader_code = record.getStr("leader_code");
                ByteArrayInputStream byteArrayInputStream = IoUtil.toStream(aphoto01s);
                //地址需要我们专门设置
                File file = new File("/home/photos/" + leader_code + fileType);
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                IoUtil.copy(byteArrayInputStream, fileOutputStream);
                byteArrayInputStream.close();
                fileOutputStream.close();

                saveA01Record.set("A0000", leader_code);
                saveA01Record.set("A0198", "/upload/impFile/Photos/" + leader_code + DateUtil.format(new Date(),"yyyy-MM-dd HH:mm:ssSSS") + fileType);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public  String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }


    public  String checkType(String xxxx) {

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 6 && StrUtil.equalsIgnoreCase(xxxx.substring(0,6),"FFD8FF")){
            return ".jpg";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 6 && StrUtil.equalsIgnoreCase(xxxx.substring(0,6),"89504E")){
            return ".png";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 6 && StrUtil.equalsIgnoreCase(xxxx.substring(0,6),"474946")){
            return ".jif";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 8 && StrUtil.equalsIgnoreCase(xxxx.substring(0,8),"000001BA")){
            return ".mpg";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 8 && StrUtil.equalsIgnoreCase(xxxx.substring(0,8),"000001B3")){
            return ".mpg";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 8 && StrUtil.equalsIgnoreCase(xxxx.substring(0,8),"47494638")){
            return ".gif";
        }

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 8 && StrUtil.equalsIgnoreCase(xxxx.substring(0,8),"49492A00")){
            return ".tif";
        }

        return "0000";
    }


    /**
     * 处理干部全息系统用户权限
     */
    public void processQxRole(){
        //查询没有角色的人员
        List<Record> saveSysQxRoleLinkeB01SaveList = new ArrayList<>();
        List<Record> saveSysQxRolePermissionList = new ArrayList<>();
        List<Record> saveSysUserB01PermisisonList = new ArrayList<>();
        List<Record> saveSysRoleList = new ArrayList<>();
        List<Record> updateSysUserList = new ArrayList<>();
        List<Record> updateSysUserPermissionList = new ArrayList<>();
        List<Record> permissionList = Db.use(PG).find("select * from \"SysPermission\" where \"type\" = '0' and \"id\" not in ('68','70','71','215','76','77','78') and \"permissionCode\" not like '006%'  and \"permissionCode\" not like '007%' and \"permissionCode\" not like '008%' order by \"permissionCode\"");

        List<Record> records = Db.use(PG).find("select * from \"SysUserRolePermission\" where \"userID\" in (select \"id\" from \"SysUser\" where \"id\" in (select \"userId\" from \"SysUserLinkModule\" where \"moduleId\" = '0' group by \"userId\") and (\"currentUserRoleId\" not in (select \"id\" from \"SysRole\" where \"type\" = '0' ) or \"currentUserRoleId\" is null )) ");
        for (Record record : records) {
            List<Map> orgArray = com.alibaba.fastjson.JSONArray.parseArray(record.getStr("orgArray"), Map.class);
            if(ObjectUtil.isNotNull(orgArray) && orgArray.size() ==1){
                String orgId = orgArray.get(0).get("orgId").toString();
                Record b01Record = Db.use(PG).findFirst("select * from \"b01\" where \"id\" = ?", orgId);
                String roleUuid = this.saveRoleList(b01Record, saveSysRoleList);
                this.getSaveQxRoleList(b01Record,roleUuid,saveSysQxRoleLinkeB01SaveList,saveSysUserB01PermisisonList,record.getStr("userID"));
                this.getSaveQxRolePermission(roleUuid,permissionList,saveSysQxRolePermissionList);
                Record updateUpdate = new Record();
                updateUpdate.set("id",record.getStr("userID"));
                updateUpdate.set("currentUserRoleId",roleUuid);
                updateSysUserList.add(updateUpdate);

                Record updateSysRolePermission = new Record();
                updateSysRolePermission.set("id",record.getStr("id"));
                updateSysRolePermission.set("roleID",roleUuid);
                updateSysUserPermissionList.add(updateSysRolePermission);

            } else {
                if(ObjectUtil.isNotNull(record) && ObjectUtil.isNotNull(record.get("orgId"))) {
                    if (StrUtil.equalsAny(record.get("orgId").toString(), "1")) {
                        continue;
                    } else {
                        String orgId = record.get("orgId").toString();
                        Record b01Record = Db.use(PG).findFirst("select * from \"b01\" where \"id\" = ?", orgId);
                        String roleUuid = this.saveRoleList(b01Record, saveSysRoleList);
                        this.getSaveQxRoleList(b01Record, roleUuid, saveSysQxRoleLinkeB01SaveList, saveSysUserB01PermisisonList, record.getStr("userID"));
                        this.getSaveQxRolePermission(roleUuid, permissionList, saveSysQxRolePermissionList);
                        Record updateUpdate = new Record();
                        updateUpdate.set("id", record.getStr("userID"));
                        updateUpdate.set("currentUserRoleId", roleUuid);
                        updateSysUserList.add(updateUpdate);

                        Record updateSysRolePermission = new Record();
                        updateSysRolePermission.set("id", record.getStr("id"));
                        updateSysRolePermission.set("roleID", roleUuid);
                        updateSysUserPermissionList.add(updateSysRolePermission);
                    }
                }
            }
        }
        Db.use(PG).tx(()->{
            if(CollectionUtil.isNotEmpty(saveSysRoleList)){
                Db.use(PG).batchSave("SysRole",saveSysRoleList,1000);
            }
            if(CollectionUtil.isNotEmpty(saveSysUserB01PermisisonList)){
                Db.use(PG).batchSave("SysUserB01Permission",saveSysUserB01PermisisonList,1000);
            }
            if(CollectionUtil.isNotEmpty(saveSysQxRoleLinkeB01SaveList)){
                Db.use(PG).batchSave("SysQxRoleLinkB01",saveSysQxRoleLinkeB01SaveList,1000);
            }
            if(CollectionUtil.isNotEmpty(saveSysQxRolePermissionList)){
                Db.use(PG).batchSave("SysQxRoleLinkPermission",saveSysQxRolePermissionList,1000);
            }

            if(CollectionUtil.isNotEmpty(updateSysUserList)){
                Db.use(PG).batchUpdate("SysUser","id",updateSysUserList,1000);
            }
            if(CollectionUtil.isNotEmpty(updateSysUserPermissionList)){
                Db.use(PG).batchUpdate("SysUserRolePermission","id",updateSysUserPermissionList,1000);
            }

            return true;
        });
    }

    private void getSaveQxRolePermission(String roleUuid, List<Record> permissionList,List<Record> saveSysQxRolePermissionList) {
        for (Record record : permissionList) {
            Record saveRecord = new Record();
            Integer id = record.getInt("id");
            saveRecord.set("id",StrKit.getRandomUUID().toUpperCase());
            saveRecord.set("qxRoleId",roleUuid);
            saveRecord.set("permissionId",id);
            saveSysQxRolePermissionList.add(saveRecord);
        }
    }


    private String saveRoleList(Record records, List<Record> saveSysRoleList) {
        Record record = new Record();
        String uuid = StrKit.getRandomUUID().toUpperCase();
        record.set("id",uuid);
        record.set("name",records.getStr("B0104"));
        record.set("isBuiltIn",0);
        record.set("createTime",new Date());
        record.set("updateTime",new Date());
        record.set("create","admin");
        record.set("type",0);
        saveSysRoleList.add(record);
        return uuid;
    }

    private void getSaveQxRoleList(Record b01Record,String roleUuid,List<Record> saveSysQxRoleLinkeB01SaveList,List<Record> saveSysUserB01PermisisonList,String userID) {
        Record saveB01Permission = new Record();
        saveB01Permission.set("id",StrKit.getRandomUUID().toUpperCase());
        saveB01Permission.set("userId",userID);
        saveB01Permission.set("orgId",b01Record.getStr("id"));
        saveB01Permission.set("system","0");
        saveB01Permission.set("isReadOnly",0);
        saveB01Permission.set("isConfig","0");
        saveSysUserB01PermisisonList.add(saveB01Permission);


        Record record = new Record();
        record.set("id",StrKit.getRandomUUID().toUpperCase());
        record.set("qxRoleId",roleUuid);
        record.set("orgId",b01Record.getStr("id"));
        record.set("isReadOnly",0);
        saveSysQxRoleLinkeB01SaveList.add(record);
        String b0121 = b01Record.getStr("B0121");
        while (true){
            Record first = Db.use(PG).findFirst("select * from \"b01\" where \"B0111\" = ? and \"isDelete\" = 0 ", b0121);
            if(ObjectUtil.isNotNull(first)) {
                Record record2 = new Record();
                record2.set("id", StrKit.getRandomUUID().toUpperCase());
                record2.set("qxRoleId", roleUuid);
                record2.set("orgId", first.getStr("id"));
                record2.set("isReadOnly", 1);
                saveSysQxRoleLinkeB01SaveList.add(record2);
                Record saveB01Permission2 = new Record();
                saveB01Permission2.set("id",StrKit.getRandomUUID().toUpperCase());
                saveB01Permission2.set("userId",userID);
                saveB01Permission2.set("orgId",first.getStr("id"));
                saveB01Permission2.set("system","0");
                saveB01Permission2.set("isReadOnly",1);
                saveB01Permission2.set("isConfig","0");
                saveSysUserB01PermisisonList.add(saveB01Permission2);

                b0121 = first.getStr("B0121");
            }else {
                break;
            }
        }
    }

    /**
     * 同步一下身份证和A0000身份唯一标识
     */
    public void syncLeaderCode() {
        List<Record> sqlServerA01 = syncDao.findSqlServerA01Test();
        List<Record> savePgA0184MappingList = new ArrayList<>();
        for (Record record : sqlServerA01) {
            Record saveRecord = new Record();
            saveRecord.set("A0000",record.getStr("leader_code"));
            saveRecord.set("A0184",record.getStr("a0184"));
            savePgA0184MappingList.add(saveRecord);
        }
        Db.use(DBConstant.PG).tx(()->{
            Db.use(DBConstant.PG).delete("delete from \"syncArchivesA0184Mapping\"");
            if(savePgA0184MappingList.size() > 0){
                Db.use(DBConstant.PG).batchSave("syncArchivesA0184Mapping",savePgA0184MappingList,1000);
            }
            return true;
        });
    }



}
