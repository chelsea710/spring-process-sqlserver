package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.config.Exce;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import com.zenith.springprocesssqlserver.constant.DictConstant;
import com.zenith.springprocesssqlserver.sync.dao.SyncDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author LHR
 * @date 2021/4/9
 */
@Service
public class VerifyService {

    @Autowired
    private SyncDao syncDao;

    @Autowired
    private DictionaryService dictionaryService;

    /**
     * 校核同步过来的a01数据
     * @return 校核结果
     */
    public List<String> verifyA01(Record item){
        List<String> result = new ArrayList<>();
        //姓名-字段为空
        this.verifyEmpty(item, "a0101", "姓名-字段为空", result);
        //姓名-包含数字,英文,括号,空格
        this.verifyMatches(item, "a0101", "姓名-包含数字,英文,括号,空格", "[\u4e00-\u9fa5]{0,100}", result);
        //性别-字段为空
        this.verifyEmpty(item, "a0104", "性别-字段为空", result);
        //性别-不能映射至代码表
        this.verifyDictionary(item, "a0104", "性别-不能映射至代码表", DictConstant.XB_TYPE, result);
        //出生日期-字段为空
        this.verifyObjectEmpty(item, "a0107", "出生日期-字段为空", result);
        //出生日期-晚于当前时间 1
        this.verifyDateFix(item, "a0107", "today", "出生日期-晚于当前时间", result);
        //籍贯(汉字)-小于等于3个汉字	1
        this.verifyNum(item, "a0111_1", "籍贯(汉字)-小于等于3个汉字",4, result);
        //出生地(汉字)-小于等于3个汉字  1
        this.verifyNum(item, "a0114_1", "出生地(汉字)-小于等于3个汉字", 4, result);
        //民族-字段为空	 1
        this.verifyEmpty(item, "a0117", "民族-字段为空", result);
        //民族-不能映射至代码表	 1
        this.verifyDictionary(item, "a0117", "民族-不能映射至代码表", DictConstant.MZ_TYPE, result);
        //参加工作时间-字段为空	 1
        this.verifyObjectEmpty(item, "a0134", "参加工作时间-字段为空", result);
        //参加工作时间-晚于当前时间	 1
        this.verifyDateFix(item, "a0134", "today", "参加工作时间-晚于当前时间", result);
        //参加工作时间-早于出生日期	 1
        this.verifyDateFix(item, "a0107", "a0134", "参加工作时间-早于出生日期", result);
        //身份证号-字段为空	1
        this.verifyEmpty(item, "a0184", "身份证号-字段为空", result);
        //身份证号不能重复
        List<Record> recordList = syncDao.findA0184OnlyOne(item.getStr("a0184"));
        if(recordList.size() > 1){
            List<String> names = new ArrayList<>();
            for (Record record : recordList) {
                names.add(record.getStr("A0101"));
            }
            result.add("身份证号码与"+CollectionUtil.join(names,",")+"等"+names.size()+"人重复");
        }
        //身份证号-18位身份证号校验不通过	1
        this.verifyMatches(item, "a0184", "身份证号-18位身份证号校验不通过", "(^[1-9]\\d{5}(18|19|20)\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}$)",result);
        return result;
    }



    /**
     * 校核同步过来的a01数据
     * @return 校核结果
     */
    public List<String> verifyA05(Record record){
        List<String> result = new ArrayList<>();
        if(StrUtil.equalsAny(record.getStr("A0531"),"0")) {
            //职务层次-字段为空	1	select  count(1)  from   A05_temp WHERE 1=1  AND A05_temp.A0000 = :A0000 AND   (  A05_temp.A0531 is null or  A05_temp.A0531  ='')
            if (StrUtil.isEmpty(record.getStr("A0501B"))) {
                result.add("职务层次-字段为空");
            }
            //职务层次-不能映射至代码表	1	select  count(1)  from   A05_temp WHERE 1=1  AND A05_temp.A0000 = :A0000 AND  A0531 IS NOT NULL AND  A0531 <> '' AND A0531 NOT IN (SELECT code_value FROM code_value WHERE code_type in (select code_type from code_table_col where col_code = 'A0531'))
            if (StrUtil.isNotEmpty(record.getStr("A0501B")) && !dictionaryService.getDictionaryValue(DictConstant.ZWCC_TYPE).contains(record.getStr("A0501B"))) {
                result.add("职务层次-不能映射至代码表");
            }
            //职务层次批准日期-字段为空 1 select  count(1)  from   A05_temp WHERE 1=1  AND A05_temp.A0000 = :A0000 AND  A05_temp.A0531='0' AND (A05_temp.A0504 is null or A05_temp.A0504  ='')
            if (ObjectUtil.isNull(record.getDate("A0504"))) {
                result.add("职务层次批准日期-字段为空");
            }
        }
        if(StrUtil.equalsAny(record.getStr("A0531"),"1")) {
            //职级-字段为空	1	select  count(1)  from   A05_temp WHERE 1=1  AND A05_temp.A0000 = :A0000 AND   (  A05_temp.A0501B is null or  A05_temp.A0501B  ='')
            if (StrUtil.isEmpty(record.getStr("A0501B"))) {
                result.add("职级-字段为空");
            }
            //职级-不能映射至代码表	1	select count(1) from A01_temp,A05_temp where A05_temp.A0000 = :A0000 AND A01_temp.A0000=A05_temp.A0000 and A0531='1' and A0501b IS NOT NULL and A0501b <> '' and A0501b not in (select code_value from code_value where code_type='ZB148')
            if (StrUtil.isNotEmpty(record.getStr("A0501B")) && !dictionaryService.getDictionaryValue(DictConstant.ZWZJ_TYPE).contains(record.getStr("A0501B"))) {
                result.add("职级-不能映射至代码表");
            }
            //职级批准日期-字段为空 select  count(1)  from   A05_temp WHERE 1=1  AND A05_temp.A0000 = :A0000 AND A05_temp.A0524='1' AND (A05_temp.A0504 is null or A05_temp.A0504  ='')
            if (ObjectUtil.isNull(record.getDate("A0504"))) {
                result.add("职级批准日期-字段为空");
            }
        }
        return result;
    }


    /**
     * 校验下A08的数据
     * @param record a08过来的数据
     * @return 错误信息
     */
    public List<String> verifyA08(Record record)  {
        List<String> result = new ArrayList<>();
        //学历信息-与学位信息同时为空	1	A08	A0901B
        if(StrUtil.isEmpty(record.getStr("A0901B")) && StrUtil.isEmpty(record.getStr("A0801B")) && StrUtil.isEmpty(record.getStr("A0801A")) && StrUtil.isEmpty(record.getStr("A0901A"))){
            result.add("学历信息-与学位信息同时为空");
        }
        //学历名称-填写了学历代码，但学历名称为空	1	A08	A0801A
        if(StrUtil.isNotEmpty(record.getStr("A0801B")) && StrUtil.isEmpty(record.getStr("A0801A"))){
            result.add("学历名称-填写了学历代码，但学历名称为空");
        }
        //学历代码-填写了学历名称，但学历代码为空	1	A08	A0801B
        if(StrUtil.isNotEmpty(record.getStr("A0801A")) && StrUtil.isEmpty(record.getStr("A0801B"))){
            result.add("学历代码-填写了学历名称，但学历代码为空");
        }
        //学历代码-不能映射至代码表	1	A08	A0801B
        if(StrUtil.isNotEmpty(record.getStr("A0801B")) && !CollectionUtil.contains(dictionaryService.getDictionaryValue(DictConstant.XLDM_TYPE),record.getStr("A0801B"))){
            result.add("学历代码-不能映射至代码表");
        }
        //学位名称-填写了学位代码，但学位名称为空	1	A08	A0901A
        if(StrUtil.isNotEmpty(record.getStr("A0901B")) && StrUtil.isEmpty(record.getStr("A0901A"))){
            result.add("学位名称-填写了学位代码，但学位名称为空");
        }
        //学位名称(大专以上学历)-小于等于3个汉字	1	A08	A0901A
        if(StrUtil.isNotEmpty(record.getStr("A0801B"))
                && StrUtil.startWithAny(record.getStr("A0801B"),"1","2")
                && StrUtil.isNotEmpty(record.getStr("A0901A")) && StrUtil.isNotEmpty(record.getStr("A0901A"))
                && !record.getStr("A0901A").matches("[\\u4e00-\\u9fa5]{4,500}")
                && record.getStr("A0901A").length() <= 3){
            result.add("学位名称(大专以上学历)-小于等于3个汉字");
        }
        //学位代码-填写了学位名称，但学位代码为空	1	A08	A0901B
        if( StrUtil.isNotEmpty(record.getStr("A0901A")) && StrUtil.isEmpty(record.getStr("A0901B"))){
            result.add("学位代码-填写了学位名称，但学位代码为空");
        }

        //学位代码(大专以上学历)-不能映射至代码表	1	A08	A0901B
        //学位代码-大专及以下全日制学历出现学位	1	A08	A0901A
        if(StrUtil.isNotEmpty(record.getStr("A0801B"))){
            //学位代码-大专及以下全日制学历出现学位
            if(StrUtil.startWithAny(record.getStr("A0801B"),"3","4","6","7","8","9") ){
                if(StrUtil.isNotEmpty(record.getStr("A0901B")) && StrUtil.isNotEmpty(record.getStr("A0837")) &&record.getStr("A0837").equals("1") ){
                    result.add("学位代码-大专及以下全日制学历出现学位");
                }
            } else {
                if(StrUtil.isNotEmpty(record.getStr("A0901B"))  && !CollectionUtil.contains(dictionaryService.getDictionaryValue(DictConstant.XWDM_TYPE),record.getStr("A0901B"))){
                    result.add("学位代码-学位代码(大专以上学历)-不能映射至代码表");
                }
            }
        }

        //入学时间-时间格式错误	1	A08	A0804
        //入学时间-晚于当前时间	1	A08	A0804
        if(StrUtil.isNotEmpty(record.getStr("A0804"))){
            Date a0804 =  DateUtil.parse(record.getStr("A0804"),"yyyy-MM-dd HH:mm:ss.SSS");
            if(a0804.getTime() > System.currentTimeMillis()){
                result.add("入学时间-晚于当前时间");
            }
        }

        //学位授予时间-晚于当前时间	1	A08	A0904
        //学位授予时间-早于入学日期	1	A08	A0904
        if(StrUtil.isNotEmpty(record.getStr("A0904"))){
            Date a0904 = DateUtil.parse(record.getStr("A0904"),"yyyy-MM-dd HH:mm:ss.SSS");
            if(a0904.getTime() >System.currentTimeMillis()){
                result.add("学位授予时间-晚于当前时间");
            }
            if(StrUtil.isNotEmpty(record.getStr("A0804"))){
                Date a0804 = DateUtil.parse(record.getStr("A0804"),"yyyy-MM-dd HH:mm:ss.SSS");
                if (a0904.getTime() < a0804.getTime()){
                    result.add("学位授予时间-早于入学日期");
                }
            }
        }

        //毕业时间-时间格式错误	1	A08	A0807
        //毕（肄）业时间-早于入学时间	1	A08	A0807
        //毕（肄）业时间-晚于当前时间	1	A08	A0807
        if(StrUtil.isNotEmpty(record.getStr("A0807"))){

            Date a0807 = DateUtil.parse(record.getStr("A0807"),"yyyy-MM-dd HH:mm:ss.SSS");
            if(a0807.getTime() >System.currentTimeMillis()){
                result.add("毕（肄）业时间-晚于当前时间");
            }
            if(StrUtil.isNotEmpty(record.getStr("A0804"))){
                Date a0804 = DateUtil.parse(record.getStr("A0804"),"yyyy-MM-dd HH:mm:ss.SSS");
                if (a0807.getTime() < a0804.getTime()){
                    result.add("毕（肄）业时间-早于入学时间");
                }
            }
        }

        //学校名称-中专及以上学校名称为空	1	A08	A0901A
        //学校（单位）名称(中专及以上学历)-与所学专业同时为空	1	A08	A0814
        if(StrUtil.isNotEmpty(record.getStr("A0801B"))){
            String a0801BPre = record.getStr("A0801B").substring(0,1);
            if(StrUtil.containsAny(a0801BPre,"1","2","3","41") && StrUtil.isEmpty(record.getStr("A0801A"))){
                result.add("学校名称-中专及以上学校名称为空");
            }
            if(StrUtil.containsAny(a0801BPre,"1","2","3","41") && StrUtil.isEmpty(record.getStr("A0801A")) && StrUtil.isEmpty(record.getStr("A0827"))){
                result.add("学校（单位）名称(中专及以上学历)-与所学专业同时为空");
            }
        }

        //所学专业名称(大专及以上所学专业)-字段为空	1	A08	A0824
        if(StrUtil.isNotEmpty(record.getStr("A0801B"))){
            String a0801BPre = record.getStr("A0801B").substring(0,1);
            if(StrUtil.containsAny(a0801BPre,"1","2","3") && StrUtil.isEmpty(record.getStr("A0824"))){
                result.add("所学专业名称(大专及以上所学专业)-字段为空");
            }
        }
        //教育类别-字段为空	1	A08	A0837
        //教育类别-不能映射至代码表	1	A08	A0837
        if(StrUtil.isEmpty(record.getStr("A0837"))){
            result.add("教育类别-字段为空");
        }else {
            if(!CollectionUtil.contains(dictionaryService.getDictionaryValue("VSC007"),record.getStr("A0837"))){
                result.add("教育类别-不能映射至代码表");
            }
        }
        //学历学位输出标识-不能映射至代码表	1	A08	A0899
        if(StrUtil.isNotEmpty(record.getStr("A0899"))
                && !CollectionUtil.contains(dictionaryService.getDictionaryValue("VSC007"),record.getStr("A0899"))){
            result.add("学历学位输出标识-不能映射至代码表");
        }
        //学位名称-与学历名称同时为空	1	A08	A0801A
        if(StrUtil.isEmpty(record.getStr("A0801A")) && StrUtil.isEmpty(record.getStr("A0901A"))){
            result.add("学位名称-与学历名称同时为空");
        }
        //学位代码-与学历代码同时为空	1	A08	A0801B
        if(StrUtil.isEmpty(record.getStr("A0801B")) && StrUtil.isEmpty(record.getStr("A0901B"))){
            result.add("学位代码-与学历代码同时为空");
        }

        //全日制 学历 必须有 学历名称和 学历代码
        if(StrUtil.isNotEmpty(record.getStr("A0837")) && record.getStr("A0837").equals("1") && (StrUtil.isEmpty(record.getStr("A0801B")) || StrUtil.isEmpty(record.getStr("A0801A")))){
            result.add("全日制学历名称不能空");
        }

        //大专学历 必须有 入学时间 和毕业时间
        if(StrUtil.isNotEmpty(record.getStr("A0801B")) && StrUtil.startWithAny(record.getStr("A0801B"),"1","2","3") && (ObjectUtil.isNull(record.getDate("A0804")) || ObjectUtil.isNull(record.getDate("A0807")))) {
            result.add("大专以上学历必须有入学时间和毕业时间");
        }

        //其中一条学历学位 入学时间为空 或者 毕业时间为空
        if(StrUtil.isNotEmpty(record.getStr("A0901B")) && (ObjectUtil.isNull(record.getDate("A0804")) || ObjectUtil.isNull(record.getDate("A0807")))){
            result.add("有学位没有入学时间和毕业时间");
        }

        //大专及以上学历学位不为空专业不能为空
        if(((StrUtil.isNotEmpty(record.getStr("A0801B"))
                && StrUtil.startWithAny(record.getStr("A0801B"),"1","2","3"))
                || StrUtil.isNotEmpty(record.getStr("A0901B")))
                && StrUtil.isEmpty(record.getStr("A0827"))){
            result.add("专业代码不能为空");
        }

        //专业代码-不能映射至代码表
        if(StrUtil.isNotEmpty(record.getStr("A0827")) && !CollectionUtil.contains(dictionaryService.getDictionaryValue(DictConstant.ZYDM_TYPE),record.getStr("A0827"))){
            result.add("专业代码-不能映射至代码表");
        }

        return result;
    }


    /**
     * 校核专业技术职务信息集的信息
     * @param record 专业技术职务记录对象
     * @return 错误信息
     */
    public List<String> verifyA06(Record record) {
        List<String> result = new ArrayList<>();
        //专业技术任职资格代码-字段为空	1	A06	A0601
        //专业技术任职资格代码-不能映射至代码表	1	A06	A0601
        if(StrUtil.isEmpty(record.getStr("A0601"))){
            result.add("专业技术任职资格代码-字段为空");
        }else{
            if(StrUtil.isNotEmpty(record.getStr("A0601")) && !CollectionUtil.contains(dictionaryService.getDictionaryValue(DictConstant.ZYJSZG_TYPE),record.getStr("A0601"))){
                result.add("专业技术任职资格代码-不能映射至代码表");
            }
        }
        //专业技术任职资格名称-字段为空	1	A06	A0602
        if(StrUtil.isEmpty(record.getStr("A0602"))){
            result.add("专业技术任职资格名称-字段为空");
        }
        //专业技术职务的获得资格日期-早于参加工作时间	3	A06	A0604
        //专业技术职务的获得资格日期-晚于当前时间	1	A06	A0604
        if(StrUtil.isNotEmpty(record.getStr("A0604"))){
            Date a0604 = DateUtil.parse(record.getStr("A0604"),"yyyy-MM-dd HH:mm:ss");
            if(a0604.getTime() > System.currentTimeMillis()){
                result.add("专业技术职务的获得资格日期-晚于当前时间");
            }
        }
        return result;
    }


    public List<String> verifyA14(Record record) {
        List<String> result = new ArrayList<>();
        //奖惩撤销日期-早于批准时间	1	A14	A1424
        //奖惩撤销日期-晚于当前时间	1	A14	A1424
        if(StrUtil.isNotEmpty(record.getStr("A1424"))) {
            Date a1424 = DateUtil.parse(record.getStr("A1424"), "yyyy-MM-dd HH:mm:ss");
            if (a1424.getTime() > System.currentTimeMillis()) {
                result.add("奖惩撤销日期-晚于当前时间");
            }
            if (StrUtil.isNotEmpty(record.getStr("A1407"))) {
                Date a1407 = DateUtil.parse(record.getStr("a1407"), "yyyy-MM-dd HH:mm:ss");
                if (a1424.getTime() < a1407.getTime()) {
                    result.add("奖惩撤销日期-早于批准时间");
                }
            }
        }
        //奖惩批准日期-早于参加工作时间	3	A14	A1407
        //奖惩批准日期-晚于当前时间	1	A14	A1407
        if(StrUtil.isNotEmpty(record.getStr("A1407"))) {
            Date a1407 = DateUtil.parse(record.getStr("A1407"), "yyyy-MM-dd HH:mm:ss");
            if (a1407.getTime() > System.currentTimeMillis()) {
                result.add("奖惩批准日期-晚于当前时间");
            }
        }
        //奖惩名称-字段为空	1	A14	A1404A
        if(StrUtil.isEmpty(record.getStr("A1404A"))){
            result.add("奖惩名称-字段为空");
        }
        return result;
    }


    public List<String> verifyA15(Record record) {
        List<String> result = new ArrayList<>();
        //考核结论-字段为空	3	A15	A1517
        //考核结论-不能映射至代码表	1	A15	A1517
        if(StrUtil.isEmpty(record.getStr("A1517"))){
            result.add("考核结论-字段为空");
        }else {
            if(!CollectionUtil.contains(dictionaryService.getDictionaryValue("ZB18"),record.getStr("A1517"))){
                result.add("考核结论-不能映射至代码表");
            }
        }

        //考核年度-字段为空	1	A15	A1521
        if(ObjectUtil.isNull(record.getDate("A1521"))){
            result.add("考核年度-字段为空");
        }
        return result;
    }


    public void verifyEmpty(Record record, String fieldName, String message, List<String> result) {
        if (StrUtil.isEmpty(record.getStr(fieldName)) || StrUtil.isEmpty(record.getStr(fieldName).trim())) {
            result.add(message);
        } else if (StrUtil.isNotEmpty(record.getStr(fieldName)) && StrUtil.isEmpty(record.getStr(fieldName).trim())) {
            result.add(message);
        }
    }

    public void verifyMatches(Record record, String fieldName, String message, String rule, List<String> result) {
        if (StrUtil.isNotEmpty(record.getStr(fieldName)) && !ReUtil.isMatch(rule, record.getStr(fieldName))) {
            result.add(message);
        }
    }

    public void verifyDictionary(Record record, String fieldName, String message, String codeType, List<String> result) {
        if (StrUtil.isNotEmpty(record.get(fieldName)) && !CollectionUtil.contains(dictionaryService.getDictionaryValue(codeType), record.get(fieldName))) {
            result.add(message);
        }
    }

    /**
     * 验证某字段是否是空
     */
    public void verifyObjectEmpty(Record record, String fieldName, String message, List<String> result) {
        if (ObjectUtil.isNull(record.get(fieldName))) {
            result.add(message);
        }
    }

    public void verifyDateFix(Record record, String timeStart, String timeEnd, String message, List<String> result) {
        if (StrUtil.equals(timeEnd, "today")) {
            if (ObjectUtil.isNotNull(record.getDate(timeStart)) && record.getDate(timeStart).getTime() > System.currentTimeMillis()) {
                result.add(message);
            }
        }
        if (ObjectUtil.isNotNull(record.getDate(timeStart)) && !StrUtil.equals(timeEnd, "today") && ObjectUtil.isNotNull(record.getDate(timeEnd))) {
            if (record.getDate(timeStart).getTime() > record.getDate(timeEnd).getTime()) {
                result.add(message);
            }
        }
    }

    public void verifyNum(Record record, String fieldName, String message, int matchNum, List<String> result) {
        if (StrUtil.isNotEmpty(record.getStr(fieldName)) && record.getStr(fieldName).length() < matchNum) {
            result.add(message);
        }
    }


    public List<String> verifyA36(Record record,Record A01Record) {
        List<String> result = new ArrayList<>();

        //        家庭成员姓名-字段为空	1
        //        家庭成员姓名-小于2个汉字	1
        if(StrUtil.isEmpty(record.getStr("A3601"))){
            result.add("家庭成员姓名-字段为空");
        }else{
            String a3604A = record.getStr("A3601");
            if(a3604A.matches("[\\u4e00-\\u9fa5]{0,1}")){
                result.add("家庭成员姓名-小于2个汉字");
            }
        }


        // 家庭成员称谓-字段为空	1
        // 家庭成员称谓-本人为男，称谓出现丈夫；本人为女，称谓出现妻子	1
        if(StrUtil.isEmpty(record.getStr("A3604A"))){
            result.add("家庭成员称谓-字段为空");
        }else {
            if(StrUtil.isNotEmpty(A01Record.getStr("A0104")) && A01Record.getStr("A0104").equals("1")){
                //本人为男
                if(record.getStr("A3604A").equals("丈夫")){
                    result.add("家庭成员称谓-本人为男，称谓出现丈夫");
                }
            }else if(StrUtil.isNotEmpty(A01Record.getStr("A0104")) && A01Record.getStr("A0104").equals("2")){
                //本人为女
                if(record.getStr("A3604A").equals("妻子")){
                    result.add("家庭成员称谓-本人为女，称谓出现妻子");
                }
            }
        }


        //        家庭成员出生日期-字段为空,但未检测到（已去世）字样	3
        //        家庭成员出生日期-时间格式错误	1
        //        家庭成员出生日期-晚于当前时间	1
        //        家庭成员出生日期(长辈)-晚于本人的出生年月	3
        //        家庭成员出生日期(晚辈)-早于本人的出生年月	3
        if(ObjectUtil.isNull(record.get("A3607")) && StrUtil.isNotEmpty(record.getStr("A3611"))
                && !record.getStr("A3611").contains("已去世")){
            result.add("家庭成员出生日期-字段为空,但未检测到（已去世）字样");
        }

        if(ObjectUtil.isNotNull(record.get("A3607"))){
            Date a3607 = DateUtil.parse(record.getStr("A3607"), "yyyy.MM");
            if(a3607.getTime() > System.currentTimeMillis()){
                result.add("家庭成员出生日期-晚于当前时间");
            }
        }


        //        家庭成员政治面貌(14岁以上家庭成员)-字段为空	1
        //        家庭成员政治面貌-不能映射至代码表	1
        //        家庭成员工作单位及职务-字段为空	1
        //        家庭成员工作单位及职务-已去世人员备注不规范，出现已故、已死亡、已过世等	1
        //        家庭成员工作单位及职务-小于2个字	1
        //        家庭成员-字段为空	1
        if(ObjectUtil.isNotNull(record.getDate("A3607")) ){
            if(DateUtil.betweenYear(record.getDate("A3607"),new Date(),false) >= 14
                    && StrUtil.isEmpty(record.getStr("A3627"))){
                result.add("家庭成员政治面貌(14岁以上家庭成员)-字段为空");
            }
        }

        if( StrUtil.isNotEmpty(record.get("A3627")) && !CollectionUtil.contains(dictionaryService.getDictionaryValue("GB4762"),record.get("A3627"))){
            result.add("家庭成员政治面貌-不能映射至代码表");
        }

        if(StrUtil.isEmpty(record.getStr("A3611"))){
            result.add("家庭成员工作单位及职务-字段为空");
        }else{
            String a3611 = record.getStr("A3611");
            if(StrUtil.containsAny(a3611,"已故","已死亡","已过世")){
                result.add("家庭成员工作单位及职务-已去世人员备注不规范，出现已故、已死亡、已过世");
            }
            if(a3611.length() < 2){
                result.add("家庭成员工作单位及职务-小于2个字");
            }
        }

        return result;
    }
}
