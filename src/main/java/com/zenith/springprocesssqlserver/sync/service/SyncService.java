package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.*;
import com.jfinal.kit.PathKit;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;

import com.zenith.springprocesssqlserver.config.Exce;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.zenith.springprocesssqlserver.constant.DBConstant.*;


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





    public void processPng() throws Exception {
        //查询出需要替换照片的人员信息
        List<Record> sqlStr = Db.use(PG).find("select \"A0000\",\"gwyA0000\" from \"A01_lqxth\" where \"gwyA0000\" in (select \"A0000\" from \"a01\")");
        List<Record> recordList = Db.use(PG).find("select \"A0000\" from \"A01_lqxth_add\" ");

        List<String> a0000 = sqlStr.stream().map(var -> "'"+ var.getStr("A0000")+"'").collect(Collectors.toList());
        List<String> a00001 = recordList.stream().map(var -> "'"+var.getStr("A0000")+"'").collect(Collectors.toList());
        a0000.addAll(a00001);

        Map<String, String> updateMap = sqlStr.stream().collect(Collectors.toMap(key -> key.getStr("A0000"), value -> value.getStr("gwyA0000"), (key1, key2) -> key1));
        Set<String> insertSet = recordList.stream().map(var -> var.getStr("A0000")).collect(Collectors.toSet());

        List<Record> sqlServerPhoto = Db.use(SQLSERVER).find("select A0198,A0000 from [dbo].[A01] where A0000 in (" + CollectionUtil.join(a0000, ",") + ")");

        List<String> updateSql =new ArrayList();
        if (sqlServerPhoto.size() != 0) {
            for (Record record : sqlServerPhoto) {
                byte[] aphoto01s = record.getBytes("A0198");
                String a00002 = record.getStr("A0000");
                if (ObjectUtil.isNotNull(aphoto01s) && aphoto01s.length > 0) {
                    String fileType = this.checkType(this.bytesToHexString(aphoto01s));
                    ByteArrayInputStream byteArrayInputStream = IoUtil.toStream(aphoto01s);
                    String fileTypeFix = "viewDetails" +a00002 + fileType;
                    File file = new File("/home/gb1809_2/webapp/photo/"+fileTypeFix);
                    FileOutputStream fileOutputStream = new FileOutputStream(file);
                    IoUtil.copy(byteArrayInputStream, fileOutputStream);
                    byteArrayInputStream.close();
                    fileOutputStream.close();
                    if(updateMap.containsKey(a00002)){
                        updateSql.add("update \"a01\" set \"A0198\" = '/upload/impFile/Photos/"+fileTypeFix+"' where \"A0000\" = '"+updateMap.get(a00002)+"';");
                    } else {
                        if(insertSet.contains(a00002)){
                            updateSql.add("update \"a01\" set \"A0198\" = '/upload/impFile/Photos/"+fileTypeFix+"' where \"A0000\" = '"+a00002+"';");
                        }
                    }
                }
            }
        }
        if(CollectionUtil.isNotEmpty(updateSql)){
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/gb1809_2/webapp/photoSql.txt"));
            IoUtil.write(fileOutputStream,true,CollectionUtil.join(updateSql,"\n").getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("运行完了");

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

        if(StrUtil.isNotEmpty(xxxx) && xxxx.length() >= 4 && StrUtil.equalsIgnoreCase(xxxx.substring(0,4),"424d")){
            return ".bmp";
        }

        return "0000";
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



    //导入村社区的信息
    public void loadCommuit() throws Exception{
        String orgIdin = CollectionUtil.join(Db.use(PG).find("select * from \"villageOrgTreeConfig\"").stream().map(var -> "'" + var.getStr("orgId") + "'").collect(Collectors.toList()), ",");
        List<String> b0111Parent = Db.use(PG).find("select \"B0111\" from \"b01\" where \"id\" in (" + orgIdin + ")").stream().map(var-> " \"b01\".\"B0111\" like '" + var.getStr("B0111") + "____' ").collect(Collectors.toList());


        //key 街道名称 value 街道id
        Map<String,String> orgParentMap = Db.use(PG).find("select \"id\",\"B0104\" from \"b01\" where ("+CollectionUtil.join(b0111Parent,"or")+") and \"isDelete\" = 0 ").stream().collect(Collectors.toMap(key->key.getStr("B0104"),value->value.getStr("id"),(key1,key2)->key1));

        //村社区 map
        Map<String,List<Record>> orgMap = Db.use(PG).find("select * from \"b01\" where \"system\" = '18' and \"isDelete\" = 0 order by \"orgTreeSort\"").stream().collect(Collectors.groupingBy(var->var.getStr("B0104"),LinkedHashMap::new,Collectors.toList()));

        List<Record> saveRecordList = new ArrayList<>();

        //读取文件
        File file = new File("/home/findPicFuling/1.xlsx");
//        File file = new File("F:\\1.xlsx");
        FileInputStream fileInputStream = new FileInputStream(file);
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(fileInputStream);
        XSSFSheet sheetAt = xssfWorkbook.getSheetAt(0);
        for(int index= 3;index<sheetAt.getLastRowNum();index++){
            XSSFRow row = sheetAt.getRow(index);
            if(ObjectUtil.isNotNull(row.getCell(1)) && StrUtil.isNotEmpty(row.getCell(1).getStringCellValue())) {
                Record record = new Record();
                record.set("id", StrKit.getRandomUUID().toUpperCase());
                record.set("status","1");
                //街道名称
                String orgParentName = row.getCell(1).getStringCellValue().replaceAll(" ", "");
                if (orgParentMap.containsKey(orgParentName)) {
                    record.set("jobParentUnit", orgParentMap.get(orgParentName));
                }

                //村社区名称
                String orgName = row.getCell(3).getStringCellValue();
                if (StrUtil.equals(orgName, "青龙村")) {
                    if (StrUtil.equals(orgParentName, "清溪镇")) {
                        record.set("jobUnit", "48E9357E7BA4438A84C2C9CA684BB139");
                    } else {
                        record.set("jobUnit", "49D42422CB8141FCBDDE6FF5E55D9FE9");
                    }
                } else {
                    try {
                        record.set("jobUnit", orgMap.get(orgName.replaceAll(" ","")).get(0).getStr("id"));
                    }catch (Exception e){
                        System.out.println("错误:"+orgName);
                    }
                }

                //姓名
                String name = row.getCell(4).getStringCellValue();
                record.set("name", name);

                //性别
                String gender = row.getCell(5).getStringCellValue();
                record.set("gender", StrUtil.equals(gender, "男") ? "1" : "2");

                //出生日期与身份证号
                String idCard = row.getCell(7).getStringCellValue();
                record.set("idCard", idCard);
                String birthByIdCard = IdcardUtil.getBirthByIdCard(idCard);
                record.set("birthday", DateUtil.parse(birthByIdCard, "yyyyMMdd"));

                //正职面貌
                String political = row.getCell(8).getStringCellValue();
                record.set("political", null);
                if (StrUtil.isNotEmpty(political)) {
                    political = political.replaceAll(" ", "");
                    if (StrUtil.equalsAny(political, "党员", "中共党员")) {
                        record.set("political", "01");
                    }
                    if (StrUtil.equalsAny(political, "预备党员", "中共预备党员")) {
                        record.set("political", "02");
                    }
                    if (StrUtil.equalsAny(political, "共青团员", "团员")) {
                        record.set("political", "03");
                    }
                    if (StrUtil.equalsAny(political, "群众")) {
                        record.set("political", "13");
                    }
                }

                //入党时间
                XSSFCell cell1 = row.getCell(9);
                record.set("joinThePartTime", null);
                if(ObjectUtil.isNotNull(cell1)){
                    String joinThePartTimeStr = cell1.getStringCellValue();
                    if(StrUtil.isNotEmpty(joinThePartTimeStr)){
                        if (StrUtil.isNotEmpty(joinThePartTimeStr) && !StrUtil.containsAny(joinThePartTimeStr, "1900")) {
                            if (StrUtil.containsAny(joinThePartTimeStr, ".", "年", "-","/")) {
                                record.set("joinThePartTime", DateUtil.parse(joinThePartTimeStr, StrDateFormatUtil.getDateFormat(joinThePartTimeStr)));
                            } else {
                                try {
                                    record.set("joinThePartTime", DateUtil.parse(joinThePartTimeStr, "yyyyMMdd"));
                                }catch (Exception e){
                                    System.out.println("错误:"+joinThePartTimeStr);
                                }
                            }
                        }
                    }
                }




                //转正时间
                XSSFCell cell = row.getCell(10);
                record.set("turnaroundTime", null);
                if(ObjectUtil.isNotNull(cell)) {
                    String turnaroundTime = cell.getStringCellValue();
                    if(StrUtil.isNotEmpty(turnaroundTime)) {
                        if (StrUtil.isNotEmpty(turnaroundTime) && !StrUtil.containsAny(turnaroundTime, "1900")) {
                            if (StrUtil.containsAny(turnaroundTime, ".", "年", "-","/")) {
                                record.set("turnaroundTime", DateUtil.parse(turnaroundTime, StrDateFormatUtil.getDateFormat(turnaroundTime)));
                            } else {
                                try {
                                    record.set("turnaroundTime", DateUtil.parse(turnaroundTime, "yyyyMMdd"));
                                }catch (Exception e){
                                    System.out.println("错误:"+turnaroundTime);
                                }
                            }
                        }
                    }
                }

                //学历
                String education = row.getCell(11).getStringCellValue();
                record.set("education", education);
                if (StrUtil.isNotEmpty(education)) {
                    if (StrUtil.equalsAny(education, "本科", "大学", "大学本科及以上", "大学本科及以上学历")) {
                        record.set("education", "21");
                    }
                    if (StrUtil.equalsAny(education, "大学专科", "大专", "大专学历", "专科")) {
                        record.set("education", "31");
                    }
                    if (StrUtil.containsAny(education, "中专")) {
                        record.set("education", "41");
                    }
                    if (StrUtil.containsAny(education, "高中")) {
                        record.set("education", "61");
                    }
                    if (StrUtil.containsAny(education, "初中")) {
                        record.set("education", "71");
                    }
                }

                //专业
                String specialty = row.getCell(12).getStringCellValue();
                record.set("specialty", StrUtil.isNotEmpty(specialty) ? specialty : null);

                //是否一肩挑
                String isPartTime = row.getCell(13).getStringCellValue();
                record.set("isPartTime", null);
                if (StrUtil.containsAny(isPartTime, "是")) {
                    record.set("isPartTime", "1");
                }
                if (StrUtil.containsAny(isPartTime, "否")) {
                    record.set("isPartTime", "0");
                }

                //党内职务
                String partJob = row.getCell(14).getStringCellValue();
                record.set("partJob", null);
                if (StrUtil.containsAny(partJob, "党组织书记")) {
                    record.set("partJob", "1");
                }
                if (StrUtil.containsAny(partJob, "党组织副书记")) {
                    record.set("partJob", "2");
                }
                if (StrUtil.containsAny(partJob, "党组织委员")) {
                    record.set("partJob", "3");
                }

                //行政职务
                String executivePositions = row.getCell(15).getStringCellValue();
                record.set("executivePositions", null);
                if (StrUtil.containsAny(executivePositions, "主任") && !StrUtil.containsAny(executivePositions, "副主任")) {
                    record.set("executivePositions", "1");
                }
                if (StrUtil.containsAny(executivePositions, "副主任")) {
                    record.set("executivePositions", "2");
                }
                if (StrUtil.containsAny(executivePositions, "委员")) {
                    record.set("executivePositions", "3");
                }

                //兼任职务
                record.set("partTimePositions", null);
                String partTimePositionsOne = row.getCell(16).getStringCellValue();
                String partTimePositionsTwo = row.getCell(17).getStringCellValue();
                Set<String> partTimePositionsSet = new LinkedHashSet<>();
                if (StrUtil.containsAny(partTimePositionsOne, "服务专干")) {
                    partTimePositionsSet.add("1");
                }
                if (StrUtil.containsAny(partTimePositionsTwo, "服务专干")) {
                    partTimePositionsSet.add("1");
                }
                if (StrUtil.containsAny(partTimePositionsOne, "治理专干")) {
                    partTimePositionsSet.add("2");
                }
                if (StrUtil.containsAny(partTimePositionsTwo, "治理专干")) {
                    partTimePositionsSet.add("2");
                }
                if (StrUtil.containsAny(partTimePositionsOne, "连长")) {
                    partTimePositionsSet.add("3");
                }
                if (StrUtil.containsAny(partTimePositionsTwo, "连长")) {
                    partTimePositionsSet.add("3");
                }
                if (StrUtil.containsAny(partTimePositionsOne, "支部")) {
                    partTimePositionsSet.add("4");
                }
                if (StrUtil.containsAny(partTimePositionsTwo, "支部")) {
                    partTimePositionsSet.add("4");
                }
                if (StrUtil.containsAny(partTimePositionsOne, "妇联")) {
                    partTimePositionsSet.add("5");
                }
                if (StrUtil.containsAny(partTimePositionsTwo, "妇联")) {
                    partTimePositionsSet.add("5");
                }
                if(CollectionUtil.isNotEmpty(partTimePositionsSet)){
                    record.set("partTimePositions",CollectionUtil.join(partTimePositionsSet,","));
                }

                //是否在村挂职人才
                String isHangOut = row.getCell(18).getStringCellValue();
                record.set("isHangOut", null);
                if (StrUtil.containsAny(isHangOut, "是")) {
                    record.set("isHangOut", "1");
                }
                if (StrUtil.containsAny(isPartTime, "否")) {
                    record.set("isHangOut", "0");
                }

                //是否纪委。。。。。
                String isInspectionPartJob = row.getCell(19).getStringCellValue();
                record.set("isInspectionPartJob", null);
                if (StrUtil.isNotEmpty(isInspectionPartJob)) {
                    if (StrUtil.containsAny(isInspectionPartJob, "是")) {
                        record.set("isInspectionPartJob", "1");
                    }
                    if (StrUtil.containsAny(isInspectionPartJob, "否")) {
                        record.set("isInspectionPartJob", "0");
                    }
                }

                //干部来源
                String cadreSources = row.getCell(20).getStringCellValue();
                record.set("cadreSources", null);
                if (StrUtil.isNotEmpty(cadreSources)) {
                    if (StrUtil.containsAny(cadreSources, "人才")) {
                        record.set("cadreSources", "8");
                    } else if (StrUtil.containsAny(cadreSources, "待业大中专毕业生")) {
                        record.set("cadreSources", "3");
                    } else if (StrUtil.containsAny(cadreSources, "返乡农民工")) {
                        record.set("cadreSources", "6");
                    } else if (StrUtil.containsAny(cadreSources, "高校毕业生")) {
                        record.set("cadreSources", "2");
                    } else if (StrUtil.containsAny(cadreSources, "农村专业大户")) {
                        record.set("cadreSources", "4");
                    } else if (StrUtil.equals(cadreSources, "其他")) {
                        record.set("cadreSources", "9");
                    } else if (StrUtil.containsAny(cadreSources, "群团", "企事业单位")) {
                        record.set("cadreSources", "1");
                    } else if (StrUtil.containsAny(cadreSources, "私营企业主")) {
                        record.set("cadreSources", "5");
                    } else if (StrUtil.containsAny(cadreSources, "原任村两委")) {
                        record.set("cadreSources", "4");
                    } else if (StrUtil.containsAny(cadreSources, "物流服务企业")) {
                        record.set("cadreSources", "5");
                    } else {
                        record.set("cadreSources", cadreSources);
                    }
                }

                //任职情况
                String appointment = row.getCell(21).getStringCellValue();
                record.set("appointment",null);
                if(StrUtil.isNotEmpty(appointment)){
                    if(StrUtil.containsAny(appointment,"新进","新任")){
                        record.set("appointment","1");
                    }
                    if(StrUtil.containsAny(appointment,"转任")){
                        record.set("appointment","3");
                    }
                    if(StrUtil.containsAny(appointment,"继任")){
                        record.set("appointment","2");
                    }
                }

                //手机号
                XSSFCell phoneCell = row.getCell(22);
                record.set("phone", null);
                if(ObjectUtil.isNotNull(phoneCell)) {
                    String phone = phoneCell.getStringCellValue();
                    if(StrUtil.isNotEmpty(phone)) {
                        record.set("phone", phone);
                    }
                }

                //简历
                String a01701 = row.getCell(23).getStringCellValue();
                record.set("a1701",StrUtil.isNotEmpty(a01701)?a01701:null);
                //备注
                String mark = row.getCell(24).getStringCellValue();
                record.set("mark",StrUtil.isNotEmpty(mark)?mark:null);

                saveRecordList.add(record);

            }

        }

        if(CollectionUtil.isNotEmpty(saveRecordList)){
            Db.use(DBConstant.PG).tx(()->{
                Db.use(DBConstant.PG).batchSave("villageMemInfo",saveRecordList,1000);
                return true;
            });
        }
    }


    public String getTimeStr(XSSFCell hssfCell) {
        int dformat = hssfCell.getCellStyle().getDataFormat();
        SimpleDateFormat sdf = null;
        if (Arrays.asList(14, 178, 192, 194, 208, 196, 210).contains(dformat)) {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        } else if (Arrays.asList(190, 191).contains(dformat)) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        } else if (Arrays.asList(177, 182, 185).contains(dformat)) {
            sdf = new SimpleDateFormat("yyyy年MM月dd日");
        } else if (Arrays.asList(183, 186).contains(dformat)) {
            sdf = new SimpleDateFormat("yyyy年MM月");
        } else if (Arrays.asList(183, 200, 201, 202, 203).contains(dformat)) {
            sdf = new SimpleDateFormat("HH:mm");
        } else if (Arrays.asList(204, 205, 206, 207, 208).contains(dformat)) {
            sdf = new SimpleDateFormat("HH时mm分");
        } else if (Arrays.asList(184, 187).contains(dformat)) {
            sdf = new SimpleDateFormat("MM月dd日");
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        }
        return sdf.format(hssfCell.getDateCellValue());
    }


    public void selectB0114(){
        //机构编码的问题
        List<String> b0114 = Db.use(PG).find("select \"B0114\" from \"b01\" where \"B0114\" is not null and \"B0114\" <> '' and \"isDelete\" = 0")
                .stream().map(var -> "'" + var.getStr("B0114") + "'").collect(Collectors.toList());


        List<Record> sqlserverB01 = Db.use(SQLSERVER).find("select b0114 from [区县02涪陵区].[dbo].[b01] where b0114 in (" + CollectionUtil.join(b0114, ",") + ") and b0114 is not null and b0114 <> ''");

        List<String> b01141 = sqlserverB01.stream().map(var -> var.getStr("b0114")).collect(Collectors.toList());

        List<Record> saveRecordList = new ArrayList<>();
        for (String s : b0114) {
            if(!b01141.contains(s.replaceAll("'",""))){
                Record record = new Record().set("B0114",s.replaceAll("'",""));
                saveRecordList.add(record);
            }
        }

        if(CollectionUtil.isNotEmpty(saveRecordList)) {
            Db.use(PG).delete("delete from \"b01_re\"");
            Db.use(PG).batchSave("b01_re", saveRecordList, 1000);
        }


        //人员身份证的问题
        List<Record> a0184List = Db.use(PG).find("select \"a01\".\"A0184\" from \"a01\" inner join \"a02\" on \"a01\".\"A0000\" = \"a02\".\"A0000\" and \"a02\".\"A0255\" = '1' inner join \"b01\" on \"a02\".\"A0201B\" = \"b01\".\"id\" and \"b01\".\"isDelete\" = 0 where \"a01\".\"A0184\" is not null and \"a01\".\"A0184\" <> ''");

        List<String> a0184 = a0184List.stream().map(var -> "'" + var.getStr("A0184").toUpperCase() + "'").collect(Collectors.toList());

        List<List<String>> list = this.subList(1000, a0184);
        Set<String> a0184Set = new HashSet<>();

        for (List<String> strings : list) {
            a0184Set.addAll(Db.use(SQLSERVER).find("select *,upper(a0184) as a0184UpCase from [区县02涪陵区].[dbo].[a01] where upper(a0184) in ("+CollectionUtil.join(strings,",")+")")
            .stream().map(var->var.getStr("a0184UpCase")).collect(Collectors.toSet()));
        }

        List<Record> saveRecordA0184List = new ArrayList<>();
        for (Record record : a0184List) {
            String pgA0184 = record.getStr("A0184").toUpperCase();
            if(!a0184Set.contains(pgA0184)){
                Record recorda0184 = new Record();
                recorda0184.set("id",StrKit.getRandomUUID().toUpperCase());
                recorda0184.set("A0184",pgA0184);
                saveRecordA0184List.add(recorda0184);
            }
        }

        if(CollectionUtil.isNotEmpty(saveRecordA0184List)) {
            Db.use(PG).delete("delete from \"a01_re\"");
            Db.use(PG).batchSave("a01_re", saveRecordA0184List, 1000);
        }
        System.out.println("完");
    }



    /**
     * List分页
     */
    public  List<List<String>> subList(Integer pageSize, List<String> list) {
        List<List<String>> result = new ArrayList<>();
        int subSize = pageSize;
        int subCount = list.size();
        int subPageTotal = (subCount / subSize) + ((subCount % subSize > 0) ? 1 : 0);
        // 根据页码取数据
        for (int i = 0, len = subPageTotal - 1; i <= len; i++) {
            // 分页计算
            int fromIndex = i * subSize;
            int toIndex = ((i == len) ? subCount : ((i + 1) * subSize));
            List<String> strings = list.subList(fromIndex, toIndex);
            result.add(strings);
        }
        return result;
    }


    public void processA1701(){
        List<Record> records = Db.use(PG).find("select * from \"a01_a1701\"");
        for (Record record : records) {
            String a1701 = record.getStr("A1701");
            if(StrUtil.isNotEmpty(a1701)){
                String[] a1701Arr = a1701.split("\n");
                //段落
                List<Map<String,String>> grapList = new ArrayList<>();
                for (String s : a1701Arr) {

                    if(grapList.size() == 0){
                        if(s.length() > 18){
                            String startStr = s.substring(0, 7);
                            String __Str = s.substring(7, 9);
                            String endStr = s.substring(9, 16);
                            String spaceStr = s.substring(16, 18);
                            if(ReUtil.isMatch("(^\\d{4}(\\.)\\d{1,2}$)", startStr) && ReUtil.isMatch("(^\\d{4}(\\.)\\d{1,2}$)", endStr) && StrUtil.equals(__Str,"--") &&StrUtil.equals(spaceStr,"  ")){
                                Map<String,String> resumeMap = new LinkedHashMap<>();
                                resumeMap.put("startStr",startStr);
                                resumeMap.put("endStr",endStr);
                                grapList.add(resumeMap);
                            }
                        }
                        continue;
                    }

                    if(grapList.size() == 1){
                        if(s.length() > 18){
                            String startStr = s.substring(0, 7);
                            String __Str = s.substring(7, 9);
                            String endStr = s.substring(9, 16);
                            String spaceStr = s.substring(16, 18);
                            if((ReUtil.isMatch("(^\\d{4}(\\.)\\d{1,2}$)", startStr) && ReUtil.isMatch("(^\\d{4}(\\.)\\d{1,2}$)", endStr) && StrUtil.equals(__Str,"--") &&StrUtil.equals(spaceStr,"  ")) ||
                                    (ReUtil.isMatch("(^\\d{4}(\\.)\\d{1,2}$)", startStr) && StrUtil.equals(endStr,"       ") && StrUtil.equals(__Str,"--") &&StrUtil.equals(spaceStr,"  "))){
                                Map<String,String> resumeMap = new LinkedHashMap<>();
                                resumeMap.put("startStr",startStr);
                                resumeMap.put("endStr",endStr);

                                Map<String, String> fristMap = grapList.get(0);
                                if(!StrUtil.equals(fristMap.get("endStr"),startStr)){
                                    a1701 = a1701.replace(fristMap.get("startStr")+"--"+fristMap.get("endStr")+"  ",fristMap.get("startStr")+"--"+startStr+"  ");
                                }
                                grapList.remove(0);
                                grapList.add(resumeMap);
                            }
                        }
                    }
                }
            }
            record.set("A1701",a1701);
        }

        if(CollectionUtil.isNotEmpty(records)){
            Db.use(PG).batchUpdate("a01_a1701","A0000",records,1000);
        }

    }


    //恢复备份
    public void processChangeData(){
        int batchNum = 1000;
        Db.use(PG).delete("delete from \"a01\"");
        Db.use(PG).delete("delete from \"a02\"");
        Db.use(PG).delete("delete from \"a05\"");
        Db.use(PG).delete("delete from \"a06\"");
        Db.use(PG).delete("delete from \"a08\"");
        Db.use(PG).delete("delete from \"a14\"");
        Db.use(PG).delete("delete from \"a15\"");
        Db.use(PG).delete("delete from \"a29\"");
        Db.use(PG).delete("delete from \"a30\"");
        Db.use(PG).delete("delete from \"a36\"");
        Db.use(PG).delete("delete from \"a99z1\"");
        Db.use(PG).delete("delete from \"b01\"");
        Db.use(PG).delete("delete from \"QxOrgShow\"");
        Db.use(PG).delete("delete from \"c_a01\"");
        Db.use(PG).delete("delete from \"c_a02\"");
        Db.use(PG).delete("delete from \"c_a03\"");
        Db.use(PG).delete("delete from \"c_c01\"");
        Db.use(PG).delete("delete from \"c_c02\"");
        Db.use("olap").delete("delete from \"mem_info\"");
        Db.use("olap").delete("delete from \"mem_team\"");
        Db.use("olap").delete("delete from \"mem_transfer\"");
        Db.use("olap").delete("delete from \"org_info\"");
        Db.use("olap").delete("delete from \"tj_tbsm_item_list\"");
        Db.use("olap").delete("delete from \"ext_table\"");
        List<Record> changeDataList = Db.use(PG).find("select * from \"changeData\"");
        for (Record record : changeDataList) {
            Map<String,String> orgIdMap = new HashMap<>();
            String name = record.getStr("name");
            String b0111 = record.getStr("B0111");
            System.out.println();
            System.out.println("正在执行"+name);
            System.out.println();
            boolean isJcy = false;
            if(StrUtil.containsAny(name,"jianchayuan")){
                isJcy = true;
            }
            //由于班子成员有非中共党员没办法
            if(ObjectUtil.equal(isJcy,false)) {
                List<Record> memTeamList = Db.use("olap_" + name).find("select * from \"mem_team\"");
                String fristB0111 = this.processGbDataBase(name, b0111, memTeamList,isJcy, batchNum,orgIdMap);
                this.processOlapDataBase(name, b0111, fristB0111,memTeamList,orgIdMap);
            } else {
                List<Record> memTeamList = Db.use("olap_" + name).find("select * from \"mem_team\" where \"org_level_code\" like '001.001.051%'");
                String fristB0111 = this.processGbDataBase(name, b0111, memTeamList, isJcy,batchNum,orgIdMap);
                this.processOlapJCYDataBase(name, b0111, fristB0111,memTeamList,orgIdMap);
            }
        }
    }


    //处理统计数据库的层级码
    public void processOlapJCYDataBase(String name,String b0111,String fristB0111,List<Record> memTeamList,Map<String,String> orgIdMap){
        List<Record> memInfoList = Db.use("olap_" + name).find("select * from \"mem_info\" where \"org_level_code\" like '001.001.051%'");
        for (Record memInfoRecord : memInfoList) {
            String memLevelCode = memInfoRecord.getStr("org_level_code");
            memInfoRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memInfoRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memInfoList)){
            Db.use("olap").batchSave("mem_info",memInfoList,1000);
        }

        for (Record memTeamRecord : memTeamList) {
            String memLevelCode = memTeamRecord.getStr("org_level_code");
            memTeamRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memTeamRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memTeamList)){
            Db.use("olap").batchSave("mem_team",memTeamList,1000);
        }

        List<Record> memTransferList = Db.use("olap_" + name).find("select * from \"mem_transfer\" where \"org_level_code\" like '001.001.051%'");
        for (Record memTransferRecord : memTransferList) {
            String memLevelCode = memTransferRecord.getStr("org_level_code");
            memTransferRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memTransferRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memTransferList)){
            Db.use("olap").batchSave("mem_transfer",memTransferList,1000);
        }

        List<Record> orgInfoList = Db.use("olap_" + name).find("select * from \"org_info\" where \"level_code\" like '001.001.051%'");
        for (Record orgInfoRecord : orgInfoList) {
            if(orgIdMap.containsKey(orgInfoRecord.getStr("gb_id"))){
                orgInfoRecord.set("gb_id",orgIdMap.get(orgInfoRecord.getStr("gb_id")));
            }
            String memLevelCode = orgInfoRecord.getStr("level_code");
            orgInfoRecord.set("level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            orgInfoRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(orgInfoList)){
            Db.use("olap").batchSave("org_info",orgInfoList,1000);
        }

        List<Record> extTableList = Db.use("olap_" + name).find("select * from \"ext_table\" where \"level_code\" like '001.001.051%'");
        for (Record extTableRecord : extTableList) {
            String memLevelCode = extTableRecord.getStr("level_code");
            extTableRecord.set("level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            extTableRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(extTableList)){
            Db.use("olap").batchSave("ext_table",extTableList,1000);
        }

        List<Record> tbsmItemList = Db.use("olap_" + name).find("select * from \"tj_tbsm_item_list\" where \"UNID\" like '001.001.051%'");
        for (Record tbsmRecord : tbsmItemList) {
            String memLevelCode = tbsmRecord.getStr("UNID");
            tbsmRecord.set("UNID",memLevelCode.replaceFirst(fristB0111,b0111));
            tbsmRecord.set("TTIT000",StrUtil.uuid().toUpperCase());
        }
        if(CollectionUtil.isNotEmpty(tbsmItemList)){
            Db.use("olap").batchSave("tj_tbsm_item_list",tbsmItemList,1000);
        }
    }



    //处理统计数据库的层级码
    public void processOlapDataBase(String name,String b0111,String fristB0111,List<Record> memTeamList,Map<String,String> orgIdMap){
        List<Record> memInfoList = Db.use("olap_" + name).find("select * from \"mem_info\" ");
        for (Record memInfoRecord : memInfoList) {
            String memLevelCode = memInfoRecord.getStr("org_level_code");
            memInfoRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memInfoRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memInfoList)){
            Db.use("olap").batchSave("mem_info",memInfoList,1000);
        }

        for (Record memTeamRecord : memTeamList) {
            String memLevelCode = memTeamRecord.getStr("org_level_code");
            memTeamRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memTeamRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memTeamList)){
            Db.use("olap").batchSave("mem_team",memTeamList,1000);
        }

        List<Record> memTransferList = Db.use("olap_" + name).find("select * from \"mem_transfer\"");
        for (Record memTransferRecord : memTransferList) {
            String memLevelCode = memTransferRecord.getStr("org_level_code");
            memTransferRecord.set("org_level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            memTransferRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(memTransferList)){
            Db.use("olap").batchSave("mem_transfer",memTransferList,1000);
        }

        List<Record> orgInfoList = Db.use("olap_" + name).find("select * from \"org_info\"");
        for (Record orgInfoRecord : orgInfoList) {
            if(orgIdMap.containsKey(orgInfoRecord.getStr("gb_id"))){
                orgInfoRecord.set("gb_id",orgIdMap.get(orgInfoRecord.getStr("gb_id")));
            }
            String memLevelCode = orgInfoRecord.getStr("level_code");
            orgInfoRecord.set("level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            orgInfoRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(orgInfoList)){
            Db.use("olap").batchSave("org_info",orgInfoList,1000);
        }

        List<Record> extTableList = Db.use("olap_" + name).find("select * from \"ext_table\"");
        for (Record extTableRecord : extTableList) {
            String memLevelCode = extTableRecord.getStr("level_code");
            extTableRecord.set("level_code",memLevelCode.replaceFirst(fristB0111,b0111));
            extTableRecord.remove("id");
        }
        if(CollectionUtil.isNotEmpty(extTableList)){
            Db.use("olap").batchSave("ext_table",extTableList,1000);
        }

        List<Record> tbsmItemList = Db.use("olap_" + name).find("select * from \"tj_tbsm_item_list\"");
        for (Record tbsmRecord : tbsmItemList) {
            String memLevelCode = tbsmRecord.getStr("UNID");
            tbsmRecord.set("UNID",memLevelCode.replaceFirst(fristB0111,b0111));
            tbsmRecord.set("TTIT000",StrUtil.uuid().toUpperCase());
        }
        if(CollectionUtil.isNotEmpty(tbsmItemList)){
            Db.use("olap").batchSave("tj_tbsm_item_list",tbsmItemList,1000);
        }
    }


    //处理干部数据库的层级码
    public String processGbDataBase(String name,String b0111,List<Record> memTeamList,boolean isJcy,Integer batchNum,Map<String,String> orgIdMap){
        //1:除了 公务员 中层正职 还有 班子成员的人
        String jcyLike = "";
        if(isJcy){
            jcyLike = " \"b01\".\"B0111\" like '001.001.051%' ";
        }
        Set<String> memTeamA000Set = memTeamList.stream().filter(var -> StrUtil.isNotEmpty(var.getStr("gb_mem_id"))).map(var -> var.getStr("gb_mem_id")).collect(Collectors.toSet());
        Set<String> containsA0000List = new HashSet<>();
        List<Record> a01SaveList = new ArrayList<>();
        List<Record> a01List;
        if(StrUtil.containsAny(name,"qijiang")){
            a01List = Db.use("gb_" + name).find("select * from \"a01\" where \"A0000\" not in ('25E960EE-6677-4DC9-8853-D83631156160') ");
        } else {
            a01List = Db.use("gb_" + name).find("select * from \"a01\" ");
        }
        List<Record> a0000List = Db.use("gb_" + name).find("select \"a01\".\"A0000\" from \"a01\" " +
                "inner join \"a02\" on \"a01\".\"A0000\" = \"a02\".\"A0000\" and \"a02\".\"A0255\" = '1' " +
                "inner join \"b01\" on \"a02\".\"A0201B\" = \"b01\".\"id\" and \"b01\".\"isDelete\" = 0 " + (StrUtil.isNotEmpty(jcyLike)?" where "+jcyLike:"") +
                "group by \"a01\".\"A0000\"");
        Set<String> a0000Set = a0000List.stream().filter(var -> StrUtil.isNotEmpty(var.getStr("A0000"))).map(var->var.getStr("A0000")).collect(Collectors.toSet());
        for (Record record : a01List) {
            if(a0000Set.contains(record.getStr("A0000")) && (StrUtil.equalsAny(record.getStr("A0160"),"1","5","6") || StrUtil.equals(record.getStr("A01Z110"),"1") || memTeamA000Set.contains(record.getStr("A0000")))){
                a01SaveList.add(record);
                containsA0000List.add(record.getStr("A0000"));
            }
        }
        if(CollectionUtil.isNotEmpty(a01SaveList)) {
            Db.use(PG).batchSave("a01", a01SaveList, batchNum);
        }

        List<Record> a02SaveList = new ArrayList<>();
        List<Record> a02List = Db.use("gb_" + name).find("select * from \"a02\" where \"A0255\" = '1' ");
        for (Record record : a02List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a02SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a02SaveList)) {
            Db.use(PG).batchSave("a02", a02SaveList, batchNum);
        }

        List<Record> a05SaveList = new ArrayList<>();
        List<Record> a05List = Db.use("gb_" + name).find("select * from \"a05\"  ");
        for (Record record : a05List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a05SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a05SaveList)) {
            Db.use(PG).batchSave("a05", a05SaveList, batchNum);
        }


        List<Record> a06SaveList = new ArrayList<>();
        List<Record> a06List = Db.use("gb_" + name).find("select * from \"a06\" ");
        for (Record record : a06List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a06SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a06SaveList)) {
            Db.use(PG).batchSave("a06", a06SaveList, batchNum);
        }

        List<Record> a08SaveList = new ArrayList<>();
        List<Record> a08List = Db.use("gb_" + name).find("select * from \"a08\" ");
        for (Record record : a08List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a08SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a08SaveList)) {
            Db.use(PG).batchSave("a08", a08SaveList, batchNum);
        }

        List<Record> a14SaveList = new ArrayList<>();
        List<Record> a14List = Db.use("gb_" + name).find("select * from \"a14\" ");
        for (Record record : a14List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a14SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a14SaveList)) {
            Db.use(PG).batchSave("a14", a14SaveList, batchNum);
        }


        List<Record> a15SaveList = new ArrayList<>();
        List<Record> a15List = Db.use("gb_" + name).find("select * from \"a15\" ");
        for (Record record : a15List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a15SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a15SaveList)) {
            Db.use(PG).batchSave("a15", a15SaveList, batchNum);
        }

        List<Record> a29SaveList = new ArrayList<>();
        List<Record> a29List = Db.use("gb_" + name).find("select * from \"a29\" ");
        for (Record record : a29List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a29SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a29SaveList)) {
            Db.use(PG).batchSave("a29", a29SaveList, batchNum);
        }

        List<Record> a30SaveListList = new ArrayList<>();
        List<Record> a30List = Db.use("gb_" + name).find("select * from \"a30\" ");
        for (Record record : a30List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a30SaveListList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a30SaveListList)) {
            Db.use(PG).batchSave("a30", a30SaveListList, batchNum);
        }


        List<Record> a36SaveList = new ArrayList<>();
        List<Record> a36List = Db.use("gb_" + name).find("select * from \"a36\" ");
        for (Record record : a36List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a36SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a36SaveList)) {
            Db.use(PG).batchSave("a36", a36SaveList, batchNum);
        }

        List<Record> a99z1SaveList = new ArrayList<>();
        List<Record> a99z1List = Db.use("gb_" + name).find("select * from \"a99z1\" ");
        for (Record record : a99z1List) {
            if(containsA0000List.contains(record.getStr("A0000"))){
                a99z1SaveList.add(record);
            }
        }
        if(CollectionUtil.isNotEmpty(a99z1SaveList)) {
            Db.use(PG).batchSave("a99z1", a99z1SaveList, batchNum);
        }

        List<Record> b01List = Db.use("gb_" + name).find("select * from \"b01\" where 1=1  "+(StrUtil.isNotEmpty(jcyLike)?" and "+jcyLike:"")+" order by \"B0111\"");
        Record fristB01 = b01List.get(0);
        String fristB0111Str = fristB01.getStr("B0111");
        for (Record b01Record : b01List) {
            b01Record.set("B0111",b01Record.getStr("B0111").replaceFirst(fristB0111Str,b0111));
            if(StrUtil.equals(b01Record.getStr("B0121"),"-1")){
                int i = b0111.lastIndexOf(".");
                b01Record.set("B0121",b0111.substring(0,i));
            } else {
                b01Record.set("B0121",b01Record.getStr("B0121").replaceFirst(fristB0111Str,b0111));
            }
            if(StrUtil.equals(b01Record.getStr("id"),"1")){
                String uuid = StrKit.getRandomUUID().toUpperCase();
                orgIdMap.put("1",uuid);
                b01Record.set("id",uuid);
            }
        }

        if(CollectionUtil.isNotEmpty(b01List)) {
            Db.use(PG).batchSave("b01", b01List, batchNum);
        }

        List<Record> qxOrgShowList = Db.use("gb_" + name).find("select * from \"QxOrgShow\" where \"type\" = '1'");
        if(CollectionUtil.isNotEmpty(qxOrgShowList)){
            Db.use(PG).batchSave("QxOrgShow",qxOrgShowList,batchNum);
        }

        List<Record> ca01List = Db.use("gb_" + name).find("select * from \"c_a01\" ");
        if(CollectionUtil.isNotEmpty(ca01List)){
            Db.use(PG).batchSave("c_a01",ca01List,batchNum);
        }

        List<Record> ca02List = Db.use("gb_" + name).find("select * from \"c_a02\" ");
        if(CollectionUtil.isNotEmpty(ca02List)){
            Db.use(PG).batchSave("c_a02",ca02List,batchNum);
        }

        List<Record> ca03List = Db.use("gb_" + name).find("select * from \"c_a03\" ");
        if(CollectionUtil.isNotEmpty(ca03List)){
            Db.use(PG).batchSave("c_a03",ca03List,batchNum);
        }

        List<Record> cc01List = Db.use("gb_" + name).find("select * from \"c_c01\"");
        if(CollectionUtil.isNotEmpty(cc01List)){
            Db.use(PG).batchSave("c_c01",cc01List,batchNum);
        }

        List<Record> cc02List = Db.use("gb_" + name).find("select * from \"c_c02\"");
        if(CollectionUtil.isNotEmpty(cc02List)){
            Db.use(PG).batchSave("c_c02",cc02List,batchNum);
        }


        return fristB0111Str;
    }



    public void uploadPic() throws Exception {
        List<Record> records = Db.use(PG).find("select * from \"changeData\"");
        int index = 1;
        for (Record record : records) {
            String name = record.getStr("name");
            List<Record> memTeamList = Db.use("olap_" + name).find("select * from \"mem_team\"");
            Set<String> memTeamA000Set = memTeamList.stream().filter(var -> StrUtil.isNotEmpty(var.getStr("gb_mem_id"))).map(var -> var.getStr("gb_mem_id")).collect(Collectors.toSet());
            List<Record> a01List = Db.use("gb_" + name).find("select \"A0198\",\"A0160\",\"A01Z110\" from \"a01\" where \"A0198\" is not null and \"A0198\" <> ''");
            System.out.println();
            System.out.println(index+"正在下载"+name+"照片数据");
            for (Record a01Record : a01List) {
                if(StrUtil.equalsAny(a01Record.getStr("A0160"),"1","5","6") || StrUtil.equals(a01Record.getStr("A01Z110"),"1") || memTeamA000Set.contains(a01Record.getStr("A0000"))){
                    String a0198 = a01Record.getStr("A0198");
                    File file = new File("/home/" + name + "/gb1809/webapp" +a0198);
                    if(ObjectUtil.isNotNull(file) && file.exists()){
                        FileInputStream fileInputStream = new FileInputStream(file);
                        FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/return/Photos/"+file.getName()));
                        IoUtil.copy(fileInputStream,fileOutputStream);
                        fileInputStream.close();
                        fileOutputStream.close();
                    }
                }
            }
            System.out.println();
            System.out.println(index+name+"照片数据下载完成");
            index++;
        }
    }

    public void resultDeleteFrist(){

        //先删除所有不在统计系统的人员
        List<Record> recordList = Db.use(OLAP).find("select \"gb_mem_id\" AS \"A0000\" from \"mem_info\" where \"gb_mem_id\" is not null");
        List<Record> recordList1 = Db.use(OLAP).find("select \"gb_mem_id\" AS \"A0000\" from \"mem_team\" where \"gb_mem_id\" is not null");
        Db.use(PG).delete("delete from \"a01_pro\"");
        if(CollectionUtil.isNotEmpty(recordList)){
            Db.use(PG).batchSave("a01_pro",recordList,1000);
        }
        if(CollectionUtil.isNotEmpty(recordList1)){
            Db.use(PG).batchSave("a01_pro",recordList1,1000);
        }
        //中管干部去掉
        List<Record> recordList2 = Db.use(PG).find("select \"a01\".\"A0000\" from \"a01\" inner join \"a02\" on \"a01\".\"A0000\" = \"a02\".\"A0000\" and \"a02\".\"A0255\" = '1' " +
                " inner join \"b01\" on \"b01\".\"id\" = \"a02\".\"A0201B\" and \"b01\".\"isDelete\" = 0 where \"b01\".\"B0111\" like ? group by \"a01\".\"A0000\"", "001.001.008%");
        if(CollectionUtil.isNotEmpty(recordList2)){
            Db.use(PG).batchSave("a01_pro",recordList2,1000);
        }

        Db.use(PG).delete("delete from \"a01\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a02\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a05\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a06\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a08\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a14\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a15\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a36\" where \"A0000\" not in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a99z1\" where \"A0000\" not in (select * from \"a01_pro\")");

        List<Record> changeDataList = Db.use(PG).find("select * from \"changeData\"");
        for (Record record : changeDataList) {
            Map<String, String> orgIdMap = new HashMap<>();
            String name = record.getStr("name");
            String b0111 = record.getStr("B0111");
            Db.use(PG).delete("delete from \"b01\" where \"B0111\" like ?",b0111+"%");
        }

    }

    /**
     * 最后的合并
     */
    public void result() {

        this.resultDeleteFrist();
        //合并分节点数据
        List<Record> a01RecordList = Db.use("gb_2020_pro").find("select * from \"a01\"");
        Db.use(PG).delete("delete from \"a01_nodeData\"");
        Db.use(PG).batchSave("a01_nodeData",a01RecordList,1000);
        //重复A0000的人员
        List<Record> replace = Db.use(PG).find("select \"A0000\" from \"a01\" where \"A0000\" in (select \"A0000\" from \"a01_nodeData\")");
        //哪里统计了这些就哪里保留
        Db.use("olap").delete("delete from \"a01_pro\"");
        Db.use("olap").batchSave("a01_pro",replace,1000);
        Db.use("olap_2021_pro").delete("delete from \"a01_pro\"");
        Db.use("olap_2021_pro").batchSave("a01_pro",replace,1000);
        //中心节点统计了 分节点删除
        List<Record> records = Db.use("olap").find("select \"gb_mem_id\" as \"A0000\" from \"mem_info\" where \"change_status\" in (0,1) and \"gb_mem_id\" in (select * from \"a01_pro\")");
        List<Record> nodeRecords = Db.use("olap_2021_pro").find("select \"gb_mem_id\" as \"A0000\" from \"mem_info\" where \"change_status\" in (0,1) and \"gb_mem_id\" in (select * from \"a01_pro\")");
        //如果都统计了就提醒
        List<String> dontNotList = new ArrayList<>();
        List<String> replaceList = new ArrayList<>();
        Set<String> zxSet = records.stream().map(var -> var.getStr("A0000")).collect(Collectors.toSet());
        Set<String> nodeSet = nodeRecords.stream().map(var -> var.getStr("A0000")).collect(Collectors.toSet());
        for (Record record : replace) {
            String a0000 = record.getStr("A0000");
            if(zxSet.contains(a0000) && nodeSet.contains(a0000)){
                replaceList.add(a0000);
            }
            if(!zxSet.contains(a0000) && !nodeSet.contains(a0000)){
                dontNotList.add(a0000);
            }
        }
        if(CollectionUtil.isNotEmpty(replaceList)){
            System.out.println("重复"+CollectionUtil.join(replaceList.stream().map(var-> "'"+var+"'").collect(Collectors.toList()), ","));
            System.exit(0);
        }

        //分节点统计了 中心节点就删除 没有重复的情况
        Db.use("gb_2020_pro").delete("delete from \"a01_pro\"");
        Db.use("gb_2020_pro").batchSave("a01_pro",records,1000);
        Db.use("gb_2020_pro").delete("delete from \"a01\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a02\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a05\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a06\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a08\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a14\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a15\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a29\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a36\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use("gb_2020_pro").delete("delete from \"a99z1\" where \"A0000\" in (select * from \"a01_pro\")");
        //中心节点统计了 分节点就删除 没有重复的情况
        Db.use(PG).delete("delete from \"a01_pro\"");
        Db.use(PG).batchSave("a01_pro",nodeRecords,1000);
        Db.use(PG).delete("delete from \"a01\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a02\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a05\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a06\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a08\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a14\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a15\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a29\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a30\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a36\" where \"A0000\" in (select * from \"a01_pro\")");
        Db.use(PG).delete("delete from \"a99z1\" where \"A0000\" in (select * from \"a01_pro\")");

        //还有一种情况两边都没统计 留中心节点
        if(CollectionUtil.isNotEmpty(dontNotList)){
            List<Record> recordList = new ArrayList<>();
            for (String s : dontNotList) {
                Record record = new Record();
                record.set("A0000",s);
                recordList.add(record);
            }
            Db.use(PG).batchSave("a01_replace",recordList,1000);
            List<String> collect = dontNotList.stream().map(var -> "'" + var + "'").collect(Collectors.toList());
            Db.use("gb_2020_pro").delete("delete from \"a01\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a02\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a05\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a06\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a08\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a14\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a15\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a29\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a36\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
            Db.use("gb_2020_pro").delete("delete from \"a99z1\" where \"A0000\" in ("+CollectionUtil.join(collect,",")+")");
        }

        a01RecordList = Db.use("gb_2020_pro").find("select * from \"a01\"");
        Db.use(PG).batchSave("a01",a01RecordList,1000);

        List<Record> a02RecordList = Db.use("gb_2020_pro").find("select * from \"a02\"");
        for (Record record : a02RecordList) {
            record.set("A0200",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a02",a02RecordList,1000);

        List<Record> a05recordList = Db.use("gb_2020_pro").find("select * from \"a05\"");
        for (Record record : a05recordList) {
            record.set("A0500",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a05",a05recordList,1000);

        List<Record> a06recordList = Db.use("gb_2020_pro").find("select * from \"a06\"");
        for (Record record : a06recordList) {
            record.set("A0600",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a06",a06recordList,1000);

        List<Record> a08recordList = Db.use("gb_2020_pro").find("select * from \"a08\"");
        for (Record record : a08recordList) {
            record.set("A0800",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a08",a08recordList,1000);

        List<Record> a14recordList = Db.use("gb_2020_pro").find("select * from \"a14\"");
        for (Record record : a14recordList) {
            record.set("A1400",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a14",a14recordList,1000);

        List<Record> a15recordList = Db.use("gb_2020_pro").find("select * from \"a15\"");
        for (Record record : a15recordList) {
            record.set("A1500",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a15",a15recordList,1000);

        List<Record> a29recordList = Db.use("gb_2020_pro").find("select * from \"a29\"");
        for (Record record : a29recordList) {
            record.set("A2900",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a29",a29recordList,1000);

        List<Record> a36recordList = Db.use("gb_2020_pro").find("select * from \"a36\"");
        for (Record record : a36recordList) {
            record.set("A3600",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a36",a36recordList,1000);

        List<Record> a30RecordList = Db.use("gb_2020_pro").find("select * from \"a30\"");
        for (Record record : a30RecordList) {
            record.set("A3000",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a30",a30RecordList,1000);

        List<Record> a99z1recordList = Db.use("gb_2020_pro").find("select * from \"a99z1\"");
        for (Record record : a99z1recordList) {
            record.set("A99Z100",StrUtil.uuid().toUpperCase());
        }
        Db.use(PG).batchSave("a99z1",a99z1recordList,1000);

        List<Record> b01List = Db.use("gb_2020_pro").find("select * from \"b01\"");
        Db.use(PG).delete("delete from \"b01_pro\"");
        Db.use(PG).batchSave("b01_pro",b01List,1000);
        Db.use(PG).delete("delete from \"b01\" where \"id\" in (select \"id\" from \"b01_pro\"  )");


        List<Record> b01AfterrecordList = Db.use(PG).find("select * from \"b01\"");
        Db.use(OLAP).delete("delete from \"b01_pro\"");
        Db.use(OLAP).batchSave("b01_pro",b01AfterrecordList,1000);
        Db.use(OLAP).delete("delete from \"org_info\" where \"gb_id\" not in (select \"id\" from \"b01_pro\" where \"isDelete\" = 0 )");

        List<Record> b01recordList = Db.use("gb_2020_pro").find("select * from \"b01\"");
        Db.use(PG).batchSave("b01",b01recordList,1000);

        List<Record> QxOrgShow = Db.use("gb_2020_pro").find("select * from \"QxOrgShow\" where \"type\" = '1'");
        Db.use(PG).batchSave("QxOrgShow",QxOrgShow,1000);

        List<Record> ca01List = Db.use("gb_2020_pro").find("select * from \"c_a01\"  ");
        Db.use(PG).batchSave("c_a01",ca01List,1000);

        List<Record> ca02List = Db.use("gb_2020_pro").find("select * from \"c_a02\"  ");
        Db.use(PG).batchSave("c_a02",ca02List,1000);

        List<Record> ca03List = Db.use("gb_2020_pro").find("select * from \"c_a03\"  ");
        Db.use(PG).batchSave("c_a03",ca03List,1000);

        List<Record> cc01List = Db.use("gb_2020_pro").find("select * from \"c_c01\" ");
        Db.use(PG).batchSave("c_c01",cc01List,1000);

        List<Record> cc02List = Db.use("gb_2020_pro").find("select * from \"c_c02\"  ");
        Db.use(PG).batchSave("c_c02",cc02List,1000);


        List<Record> memInforecordList = Db.use("olap_2021_pro").find("select * from \"mem_info\"");
        for (Record record : memInforecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("mem_info",memInforecordList,1000);

        List<Record> orginforecordList = Db.use("olap_2021_pro").find("select * from \"org_info\"");
        for (Record record : orginforecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("org_info",orginforecordList,1000);

        List<Record> memteamrecordList = Db.use("olap_2021_pro").find("select * from \"mem_team\"");
        for (Record record : memteamrecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("mem_team",memteamrecordList,1000);

        List<Record> memtransferrecordList = Db.use("olap_2021_pro").find("select * from \"mem_transfer\"");
        for (Record record : memtransferrecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("mem_transfer",memtransferrecordList,1000);

        List<Record> memExtTablerecordList = Db.use("olap_2021_pro").find("select * from \"ext_table\"");
        for (Record record : memExtTablerecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("ext_table",memExtTablerecordList,1000);

        List<Record> tbsmrecordList = Db.use("olap_2021_pro").find("select * from \"tj_tbsm_item_list\"");
        for (Record record : tbsmrecordList) {
            record.remove("id");
        }
        Db.use("olap").batchSave("tj_tbsm_item_list",tbsmrecordList,1000);

        //QxOrgShow差最高级别节点
        Db.use(PG).update("insert into \"QxOrgShow\" select uuid_generate_v4(),\"id\",'1',null,null from \"b01\" where \"B0111\" in (select \"B0111\" from \"changeData\") and \"isDelete\" = 0");
        //mem_team mem_transfer 的org_id 需要调整
        Db.use(OLAP).update(" update \"mem_team\" set \"org_id\" = \"org_info\".\"id\" from \"org_info\" where \"mem_team\".\"org_level_code\" = \"org_info\".\"level_code\"");
        //辞去公职的orgCode
        Db.use(PG).update("update \"c_a01\" set \"orgCode\" = \"b01\".\"B0111\" from \"b01\" where \"c_a01\".\"unid\" = \"b01\".\"id\"");
        Db.use(OLAP).update("update \"ext_table\" set \"level_code\" = \"org_info\".\"level_code\" from \"org_info\" where \"org_info\".\"code\" = \"ext_table\".\"code\"");
    }


    /**
     * 处理公务员表的数据监测
     */
    public void calc() {
        try {
            //获取所有的Excel
            FileInputStream fileInputStream = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表.xlsx"));
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(fileInputStream);
//            FileInputStream fileInputStream2 = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表（组织专项--公务员）.xlsx"));
            FileInputStream fileInputStream2 = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表（公安专项）.xlsx"));
            XSSFWorkbook xssfWorkbook2 = new XSSFWorkbook(fileInputStream2);
            List<Record> recordList = Db.use(PG).find("select * from \"excelCalc\"");
            for (Record record : recordList) {
                Integer startRow = record.getInt("startRow");
                Integer endRow = record.getInt("endRow");
                Integer startCell = record.getInt("startCell");
                Integer endCell = record.getInt("endCell");
                String sheet = record.getStr("sheet");
                XSSFSheet sheet1 = xssfWorkbook.getSheet(sheet);
                XSSFSheet sheet2 = xssfWorkbook2.getSheet(sheet);

                for(int row = startRow;row<=endRow;row++){
                    for(int cell = startCell;cell<=endCell;cell++) {
                        try {
                            XSSFCell cell1 = sheet1.getRow(row).getCell(cell);
                            XSSFCell cell2 = sheet2.getRow(row).getCell(cell);
                            if (ObjectUtil.isNotNull(cell1) && ObjectUtil.isNotNull(cell2)) {
                                double numericCellValue = cell1.getNumericCellValue();
                                double numericCellValue1 = cell2.getNumericCellValue();
                                if (ObjectUtil.isNotNull(numericCellValue)) {
                                    if (numericCellValue - numericCellValue1 < 0) {
                                        System.out.println(sheet + "第" + (row + 1) + "行" + "第" + (cell + 1) + "列 有问题");
                                    }
                                }
                            }
                        } catch (Exception e) {
//                                e.printStackTrace();
                        }
                    }
                }
            }
            fileInputStream.close();
            fileInputStream2.close();
            System.out.println("完了");
        } catch (Exception e){
            StringWriter str = new StringWriter();
            e.printStackTrace(new PrintWriter(str));
            System.out.println(str);
        }
    }


    /**
     * 处理公务员表的数据监测
     */
    public void calc2() {
        try {
            //获取所有的Excel
            FileInputStream fileInputStream = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表.xlsx"));
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook(fileInputStream);
//            FileInputStream fileInputStream2 = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表（组织专项--公务员）.xlsx"));
            FileInputStream fileInputStream2 = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表（公安专项）.xlsx"));
            XSSFWorkbook xssfWorkbook2 = new XSSFWorkbook(fileInputStream2);

            FileInputStream fileInputStream3 = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\北碚的20220321更新\\2021年重庆市公务员统计表（组织专项--公务员）.xlsx"));
            XSSFWorkbook xssfWorkbook3 = new XSSFWorkbook(fileInputStream3);
            List<Record> recordList = Db.use(PG).find("select * from \"excelCalc\"");
            for (Record record : recordList) {
                Integer startRow = record.getInt("startRow");
                Integer endRow = record.getInt("endRow");
                Integer startCell = record.getInt("startCell");
                Integer endCell = record.getInt("endCell");
                String sheet = record.getStr("sheet");
                XSSFSheet sheet1 = xssfWorkbook.getSheet(sheet);
                XSSFSheet sheet2 = xssfWorkbook2.getSheet(sheet);
                XSSFSheet sheet3 = xssfWorkbook3.getSheet(sheet);

                for(int row = startRow;row<=endRow;row++){
                    for(int cell = startCell;cell<=endCell;cell++) {
                        try {
                            XSSFCell cell1 = sheet1.getRow(row).getCell(cell);
                            XSSFCell cell2 = sheet2.getRow(row).getCell(cell);
                            XSSFCell cell3 = sheet3.getRow(row).getCell(cell);
                            if (ObjectUtil.isNotNull(cell1) && ObjectUtil.isNotNull(cell2) && ObjectUtil.isNotNull(cell3)) {
                                double numericCellValue = cell1.getNumericCellValue();
                                double numericCellValue1 = cell2.getNumericCellValue();
                                double numericCellValue2 = cell3.getNumericCellValue();
                                if (ObjectUtil.isNotNull(numericCellValue)) {
                                    if (numericCellValue - numericCellValue1 - numericCellValue2 < 0) {
                                        System.out.println(sheet + "第" + (row + 1) + "行" + "第" + (cell + 1) + "列 有问题");
                                    }
                                }
                            }
                        } catch (Exception e) {
//                                e.printStackTrace();
                        }
                    }
                }
            }
            fileInputStream.close();
            fileInputStream2.close();
            System.out.println("完了");
        } catch (Exception e){
            StringWriter str = new StringWriter();
            e.printStackTrace(new PrintWriter(str));
            System.out.println(str);
        }
    }


    /**
     * 处理贵州组织部的字典问题
     */
    public void processExcelDict() throws Exception{
        FileInputStream inputStream = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\贵州\\贵州公务员系统同步.xlsx"));
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(inputStream);
        XSSFSheet sheetAt = xssfWorkbook.getSheetAt(0);
        Map<String,Set<String>> dictMap = new LinkedHashMap<>();
        dictMap.put("base",new LinkedHashSet<>());
        Set<String> dictSet = new HashSet<>();
        for(int rowIndex = 1;rowIndex<sheetAt.getLastRowNum();rowIndex++){
            XSSFRow row = sheetAt.getRow(rowIndex);
            if(ObjectUtil.isNotNull(row.getCell(5))) {
                String dictCodeTypeStr = row.getCell(5).getStringCellValue();
                String explainStr = row.getCell(4).getStringCellValue();
                if (StrUtil.isNotEmpty(dictCodeTypeStr)) {
                    for (String s : dictCodeTypeStr.split(",")) {
                        if(!dictSet.contains(s)) {
                            dictMap.get("base").add(explainStr + "," + s);
                            dictSet.add(s);
                        }
                    }
                }
            }
        }
        for (String dictKeySplitStr : dictMap.get("base")) {
            String dictKeyStr = dictKeySplitStr.split(",")[1];
            List<Record> dictRecordList = Db.use(PG).find("select * from \"code_value\" where \"CODE_TYPE\" = ? ORDER BY \"ININO\",\"CODE_VALUE\"", dictKeyStr);
            Set<String> subCodeValueSet = dictRecordList.stream().map(var -> var.getStr("SUB_CODE_VALUE")).collect(Collectors.toSet());
            XSSFSheet sheet = null;
            if(ObjectUtil.isNull(xssfWorkbook.getSheet(dictKeySplitStr.split(",")[0]))) {
                sheet = xssfWorkbook.createSheet(dictKeySplitStr.split(",")[0]);
            } else {
                sheet = xssfWorkbook.createSheet(dictKeySplitStr.split(",")[0]+dictKeyStr);
            }
            XSSFRow headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("名称");
            headerRow.createCell(1).setCellValue("代码");
            int rowIndex = 1;
            for (Record record : dictRecordList) {
                if(!subCodeValueSet.contains(record.getStr("CODE_VALUE"))) {
                    XSSFRow row = sheet.createRow(rowIndex);
                    row.createCell(0).setCellValue(record.getStr("CODE_NAME"));
                    row.createCell(1).setCellValue(record.getStr("CODE_VALUE"));
                    rowIndex++;
                }
            }
        }
        FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\48951\\Desktop\\贵州\\贵州公务员系统同步"+DateUtil.format(new Date(),"yyyyMMddHHmmssSSS")+".xlsx"));
        xssfWorkbook.write(fileOutputStream);
        fileOutputStream.close();
        inputStream.close();
        xssfWorkbook.close();
    }


    /**
     * 处理贵州组织部的字典问题
     */
    public void processExcelOladDict() throws Exception{
        FileInputStream inputStream = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\贵州\\贵州公务员统计系统同步.xlsx"));
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(inputStream);
        XSSFSheet sheetAt = xssfWorkbook.getSheetAt(0);
        Map<String,Set<String>> dictMap = new LinkedHashMap<>();
        dictMap.put("olap",new LinkedHashSet<>());
        Set<String> dictSet = new HashSet<>();
        for(int rowIndex = 1;rowIndex<sheetAt.getLastRowNum();rowIndex++){
            XSSFRow row = sheetAt.getRow(rowIndex);
            if(ObjectUtil.isNotNull(row.getCell(5))) {
                String dictCodeTypeStr = row.getCell(5).getStringCellValue();
                String explainStr = row.getCell(4).getStringCellValue();
                if (StrUtil.isNotEmpty(dictCodeTypeStr)) {
                    for (String s : dictCodeTypeStr.split(",")) {
                        if(!dictSet.contains(s)) {
                            dictMap.get("olap").add(explainStr + "," + s);
                            dictSet.add(s);
                        }
                    }
                }
            }
        }
        for (String dictKeySplitStr : dictMap.get("olap")) {
            String dictKeyStr = dictKeySplitStr.split(",")[1];
            List<Record> dictRecordList = Db.use(OLAP).find("select * from \"dict\" where \"type_code\" = ? ORDER BY \"order_id\",\"code\"", dictKeyStr);
            Set<String> subCodeValueSet = dictRecordList.stream().map(var -> var.getStr("parent_code")).collect(Collectors.toSet());
            XSSFSheet sheet = null;
            if(ObjectUtil.isNull(xssfWorkbook.getSheet(dictKeySplitStr.split(",")[0]))) {
                sheet = xssfWorkbook.createSheet(dictKeySplitStr.split(",")[0]);
            } else {
                sheet = xssfWorkbook.createSheet(dictKeySplitStr.split(",")[0]+dictKeyStr);
            }
            XSSFRow headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("名称");
            headerRow.createCell(1).setCellValue("代码");
            int rowIndex = 1;
            for (Record record : dictRecordList) {
                if(!subCodeValueSet.contains(record.getStr("code"))) {
                    XSSFRow row = sheet.createRow(rowIndex);
                    row.createCell(0).setCellValue(record.getStr("name"));
                    row.createCell(1).setCellValue(record.getStr("code"));
                    rowIndex++;
                }
            }
        }
        FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\48951\\Desktop\\贵州\\贵州公务员统计系统同步"+DateUtil.format(new Date(),"yyyyMMddHHmmssSSS")+".xlsx"));
        xssfWorkbook.write(fileOutputStream);
        fileOutputStream.close();
        inputStream.close();
        xssfWorkbook.close();
    }


    /**
     * 切换字段与对应的表
     */
    public void changeField() throws Exception {
        File file = new File("C:\\Users\\48951\\Desktop\\贵州\\贵州公务员系统同步20220704095457745.xlsx");
        FileInputStream inputStream = new FileInputStream(file);
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(inputStream);
        XSSFSheet sheetAt = xssfWorkbook.getSheetAt(0);
        List<Record> saveRecordList = new ArrayList<>();
        String tableName = "";
        for(int rowIndex = 1;rowIndex < sheetAt.getLastRowNum();rowIndex++){
            Record gzDataConfigRecord = new Record();
            if(StrUtil.isNotEmpty(sheetAt.getRow(rowIndex).getCell(0).getStringCellValue()) && !StrUtil.equals(sheetAt.getRow(rowIndex).getCell(0).getStringCellValue(),tableName)) {
                tableName = sheetAt.getRow(rowIndex).getCell(0).getStringCellValue();
            }
            String fieldDataBaseName = sheetAt.getRow(rowIndex).getCell(1).getStringCellValue();
            String fieldExcelName = sheetAt.getRow(rowIndex).getCell(2).getStringCellValue();
            String id = StrKit.getRandomUUID().toUpperCase();
            gzDataConfigRecord.set("tableName",tableName).set("fieldDataBaseName",fieldDataBaseName)
                    .set("fieldExcelName",fieldExcelName).set("id",id);
            saveRecordList.add(gzDataConfigRecord);
        }
        if(CollectionUtil.isNotEmpty(saveRecordList)) {
            Db.use(PG).tx(() -> {
                Db.use(PG).delete("delete from \"gzDataSyncConfig\"");
                Db.use(PG).batchSave("gzDataSyncConfig",saveRecordList,1000);
                return true;
            });
        }
    }

    public void processCodeValueBySalay(){
        List<String> fieldList = new ArrayList<>();
        fieldList.add("XZ94");
        fieldList.add("XZ95");
        fieldList.add("XZ96");
        fieldList.add("XZ97");
        fieldList.add("A3385");
        if(CollectionUtil.isNotEmpty(fieldList)) {
            Db.use(PG).delete("delete from \"code_value\" where \"CODE_TYPE\" IN (" + CollectionUtil.join(fieldList.stream().map(var -> "'" + var + "'").collect(Collectors.toList()), ",") + ")");
            for (String fieldStr : fieldList) {
                List<Record> codeValueList = Db.use(PG).find("select * from \"code_value_zzb\" where \"CODE_TYPE\" = ? order by \"ININO\",\"CODE_VALUE\"", fieldStr);
                Integer maxCodeSeq = Db.use(PG).queryInt("select max(\"CODE_VALUE_SEQ\") FROM \"code_value\"");
                maxCodeSeq++;
                for (Record record : codeValueList) {
                    record.set("CODE_VALUE_SEQ", maxCodeSeq);
                    maxCodeSeq++;
                }
                if (CollectionUtil.isNotEmpty(codeValueList)) {
                    Db.use(PG).batchSave("code_value", codeValueList, 100);
                }
            }
        }
    }

//    public void upload2019Pic() {
//        List<Record> records = Db.use(PG).find("select * from \"a01\" where \"A0184\" in (select * from \"a01_pro\")");
//        int index = 1;
//        for (Record record : records) {
//            try {
//                String a0198 = record.getStr("A0198");
//                File file = new File("/home/gb18092019/webapp" + a0198);
//                if (ObjectUtil.isNotNull(file) && file.exists()) {
//                    FileInputStream fileInputStream = new FileInputStream(file);
//                    FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/return/Photos/" + file.getName()));
//                    IoUtil.copy(fileInputStream, fileOutputStream);
//                    fileInputStream.close();
//                    fileOutputStream.close();
//                }
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//        System.out.println();
//        System.out.println("照片数据下载完成");
//        index++;
//    }

    public void upload2019Pic() {
        List<Record> changeDataList = Db.use(PG).find("select * from \"changeData\"");
        List<Record> picRecordList = Db.use(PG).find("select * from \"a01_pro\"");
        Map<String, String> proMap = picRecordList.stream().collect(Collectors.toMap(var -> var.getStr("A0184"), value -> value.getStr("A0198"), (key1, key2) -> key1));
        try {
            for (Record record : changeDataList) {
                String name = record.getStr("name");
                List<Record> updateList = new ArrayList<>();
                List<Record> recordList = Db.use(PG).find("select * from \"c_a01\"");
                for (Record record1 : recordList) {
                    if (proMap.containsKey(record1.getStr("a0184"))) {
                        String a0198 = proMap.get(record1.getStr("a0184"));
                        Record updateRecord = new Record();
                        updateRecord.set("id", record1.getStr("id"));
                        updateRecord.set("a5714", a0198);
                        updateList.add(updateRecord);
                        File file = new File("/tmp/upload/Photos/" + a0198.replaceAll("/upload/impFile/Photos/", ""));
                        FileInputStream fileInputStream = new FileInputStream(file);
                        FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/" + name + "/gb1809/webapp/upload/impFile/Photos/" + file.getName()));
                        IoUtil.copy(fileInputStream, fileOutputStream);
                        fileInputStream.close();
                        fileOutputStream.close();

                    }
                }

                if (CollectionUtil.isNotEmpty(updateList)) {
                    Db.use("gb_" + name).batchUpdate("c_a01", "id", updateList, 100);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}