package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.jfinal.kit.LogKit;
import com.jfinal.kit.PathKit;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import com.zenith.springprocesssqlserver.sync.dto.A36VO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.zenith.springprocesssqlserver.constant.DBConstant.PG;

@Service
public class ProcessYZ {

    @Autowired
    private SyncDao syncDao;


    public void test03() throws Exception{
        Db.use(PG).find("select \"\" from \"SysUserRolePermission\"");
    }

    public void test02() throws Exception {
        List<Record> recordList = Db.use(PG).find("select * from \"a14\" where \"A1404B\" = '01114' and \"A0000\" is not null and \"A1407\" is not null");
        List<String> a0000List = recordList.stream().map(var -> "'"+var.getStr("A0000")+"'").collect(Collectors.toList());
        List<Record> a15RecordList = Db.use(PG).find("select * from \"a15\" where \"A0000\" in (" + CollectionUtil.join(a0000List,",") + ") and \"A1517\" = '1' and \"A1521\" is not null ");
        LinkedHashMap<String, Map<String, Record>> a15Map = a15RecordList.stream().collect(Collectors.groupingBy(var -> var.getStr("A0000"), LinkedHashMap::new, Collectors.toMap(key -> DateUtil.format(key.getDate("A1521"), "yyyy"), value -> value, (key1, key2) -> key1)));
        for (Record record : recordList) {
            record.set("isAssessment",null);
            String a0000 = record.getStr("A0000");
            Date a1407 = record.getDate("A1407");
            String yyyy = DateUtil.format(a1407, "yyyy");
            Integer integer = Integer.valueOf(yyyy);
            if(a15Map.containsKey(a0000)){
                Map<String, Record> stringRecordMap = a15Map.get(a0000);
                if(stringRecordMap.containsKey(String.valueOf((integer-1))) && stringRecordMap.containsKey(String.valueOf((integer-2))) && stringRecordMap.containsKey(String.valueOf((integer-3)))){
                    record.set("isAssessment","1");
                }
            }
        }
        Db.use(PG).batchUpdate("a14","A1400",recordList,100);
    }


//    public void test() throws Exception {
//        File dir = new File("D:\\YUZHONG");
//        List<Record> saveA01List = new ArrayList<>();
//        File[] files = dir.listFiles();
//        if (Objects.nonNull(files)) {
//            for (File file : files) {
//                if (file.isFile()) {
//                    if(StrUtil.containsAny(file.getName(),"lrm")) {
//                        A01Vo a01Vo = this.readLrmInfo(file);
//                        Record record = new Record();
//                        record.set("id", StrKit.getRandomUUID().toUpperCase());
//                        record.set("A0101", a01Vo.getA0101());
//                        record.set("A1701", a01Vo.getA1701());
//                        saveA01List.add(record);
//                    }
//                }
//            }
//
//            if(saveA01List.size() > 0){
//                Db.use(PG).batchSave("a01_0527",saveA01List,100);
//            }
//        }
//    }
//
//
//    private A01Vo readLrmInfo(File file) throws Exception {
//        if (!"lrm".equals(FileUtil.extName(file))) {
//            return null;
//        }
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
//        StringBuilder sb = new StringBuilder();
//        String s;
//        while ((s = reader.readLine()) != null) {
//            sb.append(s).append("\n");
//        }
//        reader.close();
//        String[] shuZu = sb.toString().split("\",\"");
//        if (StrUtil.isNotEmpty(shuZu[0])) {
//            String a0101 = shuZu[0].replaceAll("\"", "").replaceAll(" ", "");
//            String a1701 = StrUtil.isNotEmpty(shuZu[17]) ? shuZu[17].replaceAll("\"", "") : "";
//            A01Vo a01Vo = new A01Vo();
//            a01Vo.setA0101(a0101);
//            a01Vo.setA1701(a1701);
//            return a01Vo;
//        }
//        return null;
//
//    }





    /**
     * 渝中区替换人员家庭成员
     */
    public void processA36Men() throws Exception {
        //读取文件
        List<A36VO> result = new ArrayList<>();
        File file = new File("D:\\YUZHONG");
        Map<String, String> gb4762KeyNameValueKey = new HashMap<>();
        Map<String, String> gb4762 = syncDao.getDicMap("GB4762");
        for (Map.Entry<String, String> entry : gb4762.entrySet()) {
            gb4762KeyNameValueKey.put(entry.getValue(), entry.getKey());
        }

        for (File lrmFile : Objects.requireNonNull(file.listFiles())) {
            if (ObjectUtil.isNotNull(lrmFile)) {
                List<A36VO> a36VOS = this.readLrmInfo(lrmFile, gb4762KeyNameValueKey);
                System.out.println(JSON.toJSON(a36VOS));
                if (CollectionUtil.isNotEmpty(a36VOS)) {
                    result.addAll(a36VOS);
                }
            }
        }
        //存入A36TempList 必须要
        List<Record> saveRecordList = new ArrayList<>();
        for (A36VO a36VO : result) {
            Record record = new Record();
            record.set("A0000", "123456");
            record.set("A0101", a36VO.getA0101());
            record.set("A0107", a36VO.getA0107());
            record.set("A3600", a36VO.getA3600());
            record.set("A3601", a36VO.getA3601());
            record.set("A3604A", a36VO.getA3604A());
            record.set("A3607", a36VO.getA3607());
            record.set("A3611", a36VO.getA3611());
            record.set("A3627", a36VO.getA3627());
            record.set("A3684", a36VO.getA3684());
            record.set("A3699", 1);
            record.set("SORTID", a36VO.getSORTID());
            record.set("UPDATED", a36VO.getUPDATED());
            saveRecordList.add(record);
        }
        if (CollectionUtil.isNotEmpty(saveRecordList)) {
            Db.use(PG).delete("delete from \"a36_0818YZ\"");
            Db.use(PG).batchSave("a36_0818YZ", saveRecordList, 1000);
        }
    }



    private List<A36VO> readLrmInfo(File file,Map<String,String> zZMMMap) throws Exception {
        List<A36VO> result = new ArrayList<>();
        if (!"lrm".equals(FileUtil.extName(file))) {
            return null;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
        StringBuilder sb = new StringBuilder();
        String s;
        while ((s = reader.readLine()) != null) {
            sb.append(s).append("\n");
        }
        reader.close();
        String[] shuZu = sb.toString().split("\",\"");

        if (StrUtil.isNotEmpty(shuZu[0])) {
            String a0101 = shuZu[0].replaceAll("\"", "").replaceAll(" ", "");
            String chusheng = shuZu[2].replaceAll("\"", "");
            String[] chengwei = shuZu[20].replaceAll("\"", "").split("@", shuZu[20].length() + 1);
            //str5
            String[] xingming = shuZu[21].replaceAll("\"", "").split("@", shuZu[21].length() + 1);
            //str6
            String[] shijian = shuZu[22].replaceAll("\"", "").split("@", shuZu[22].length() + 1);
            //str7
            String[] chengyuanzhengzhi = shuZu[23].replaceAll("\"", "").split("@", shuZu[23].length() + 1);
            //str8
            String[] gongzuo = shuZu[24].replaceAll("\"", "").split("@", shuZu[24].length() + 1);

            for (int index = 0; index <= chengwei.length - 1; ++index) {
                if (StrUtil.isNotEmpty(chengwei[index].replaceAll("\"", ""))) {
                    A36VO a36Single = new A36VO();
                    a36Single.setA0101(a0101);
                    a36Single.setA0107(this.getXmlTime(chusheng));
                    a36Single.setA0000(null);
                    a36Single.setA3600(StrKit.getRandomUUID());
                    a36Single.setA3604A(chengwei[index].replaceAll("\"", ""));
                    a36Single.setA3601(xingming[index].replaceAll("\"", ""));
                    //时间
                    String shijiancuan = shijian[index].replaceAll("\"", "");
                    a36Single.setA3607(this.getXmlTime(shijiancuan));
                    String s2 = chengyuanzhengzhi[index].replaceAll("\"", "");
                    a36Single.setA3627(this.getA36Zzmm(zZMMMap, s2));
                    a36Single.setA3611(gongzuo[index].replaceAll("\"", ""));
                    result.add(a36Single);
                } else {
                    break;
                }
            }
        }
        return result;

    }

    /**
     * 设置a36中的政治面貌
     */
    public String getA36Zzmm(Map<String, String> zZMMMap, String name) {
        String code = "";
        if (StrKit.notBlank(name)) {
            code = zZMMMap.get(name) == null ? name : zZMMMap.get(name);
        }
        return code;
    }


    /**
     * 出生日期格式化
     */
    public Date getXmlTime(String value) {
        try {
            if (StrKit.notBlank(value)) {
                value = value.replaceAll("\\.", "");
                if (value.length() == 8) {
                    return DateUtil.parse(value, "yyyyMMdd");
                } else if (value.length() == 6) {
                    return DateUtil.parse(value, "yyyyMM");
                } else if (value.length() == 4) {
                    return DateUtil.parse(value, "yyyy");
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            LogKit.error("时间格式错误:" + value, e);
        }
        return null;
    }



    public void a360818YzSort(){

        List<Record> updateListRecord = new ArrayList<>();
        List<Record> recordList = Db.use(PG).find("select * from \"a36_0818YZ\" where \"A0000\" <> '123456'");

        LinkedHashMap<String, List<Record>> a0000 = recordList.stream().collect(Collectors.groupingBy(var -> var.getStr("A0000"), LinkedHashMap::new, Collectors.toList()));
        for (Map.Entry<String, List<Record>> entry : a0000.entrySet()) {
            List<Record> value = entry.getValue();
            int sort = 1;
            for (Record record : value) {
                Record updateRecord = new Record();
                updateRecord.set("A3600",record.getStr("A3600"));
                updateRecord.set("SORTID",sort);
                sort++;
                updateListRecord.add(updateRecord);
            }
        }

        if(CollectionUtil.isNotEmpty(updateListRecord)){
            Db.use(PG).batchUpdate("a36_0818YZ","A3600",updateListRecord,100);
        }




    }

}
