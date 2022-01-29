package com.zenith.springprocesssqlserver.util;


import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.zenith.springprocesssqlserver.enum1.ResumeEnum;
import com.zenith.springprocesssqlserver.pojo.Result;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ResumeUtil {

    public static void main(String[] args) {
        ResumeUtil resumeUtil = new ResumeUtil();
        resumeUtil.checkJianLi("2003.09--2007.06  河北科技大学轻化工程专业本科学习，获工学学士学位\n" +
                "\n" +
                "2007.06--2007.09  待分配\n" +
                "\n" +
                "2007.09--2010.02  东华大学纺织化学与染整工程专业研究生学习，获工学硕士学位\n" +
                "\n" +
                "2010.02--2010.10  待分配\n" +
                "\n" +
                "2010.10--2011.11  重庆市万州区高梁镇镇长助理（挂职）、高梁镇新店村支书助理（其间：2011.09--2011.12 重庆市委党校选调生主体班培训）\n" +
                "\n" +
                "2011.11--2015.01  重庆市万州区新乡镇党委委员、宣传委员（正科级）\n" +
                "\n" +
                "2015.01--2016.10  重庆市万州区新乡镇党委委员、宣传委员\n" +
                "\n" +
                "2016.10--2017.04  重庆市万州区太龙镇党委委员、组织委员\n" +
                "\n" +
                "2017.04--2021.11  重庆市万州区太龙镇党委委员、组织委员，人大副主席（兼）（其间：2017.10--2018.02 挂任天津市宝坻区口东街道办事处副主任；2021.08--2022.09 重庆市委党校第2期选调生政治能力提升示范培训班培训）\n" +
                "\n" +
                "2021.11--         共青团重庆市万州区委员会副书记");
    }

    public String checkJianLi(String jianli) {
        if (StrUtil.isBlank(jianli)) {
            return null;
        } else {
            jianli = jianli.replaceAll("\\\\n", "\r").replaceAll("\\\\r", "\r").replaceAll("\n", "\r");
            jianli = dealJianliTextPutOffSpecial(jianli);
            List<Map<String, String>> list = getA1701List(jianli);
            Result result = checkJianLi(list);
            return "1";
        }
    }


    private Result checkJianLi(List<Map<String, String>> listmap) {
        Result res = null;
        Map<String, Object> mapTemp = null;
        List<Map<String, Object>> list = new ArrayList<>();
        String lastStart = null;
        String lastEnd = null;
        Date lastStartDate = null;
        Date lastEndDate = null;
        int i;
        for (i = 0; i < listmap.size(); i++) {
            Map<String, String> temp = listmap.get(i);
            String key = temp.get("key");
            String value = temp.get("value");
            if (!StrUtil.isBlank(key) || !StrUtil.isBlank(value))
                if (i != listmap.size() - 1 ||
                        !StrUtil.isBlank(key)) {
                    mapTemp = new HashMap<>();
                    mapTemp.put("key", key);
                    mapTemp.put("value", value);
                    list.add(mapTemp);
                }
        }
        for (i = 0; i < list.size(); i++) {
            if(i == 4){
                System.out.println("1");
            }
            mapTemp = list.get(i);
            if (i == 0)
                mapTemp.put("isFirst", Boolean.valueOf(true));
            String key = (String)mapTemp.get("key");
            String value = (String)mapTemp.get("value");
            String start = "";
            if (StrUtil.isNotBlank(key) && !key.trim().equals("--") && key.indexOf("--") != -1) {
                String[] arg = key.split("--");
                if (StrUtil.isNotBlank(arg[0])) {
                    res = checkDateString(arg[0]);
                    if (!res.isSuccess())
                        return res;
                    res = parse(arg[0]);
                    if (!res.isSuccess())
                        return res;
                    mapTemp.put("start", arg[0]);
                    mapTemp.put("startDate", res.getObj());
                    mapTemp.put("lastStartDate", lastStartDate);
                    mapTemp.put("lastStart", lastStart);
                    start = arg[0];
                    if (i != 0 &&
                            !start.equals(lastEnd))
                        return Result.fail(key + "的开始年.月需要和上一条简历中结束年.月相同!");
                    lastStart = arg[0];
                    lastStartDate = (Date)res.getObj();
                } else {
                    return Result.fail("该简历中"+ key + "的日期格式有问题");
                }
                if (arg.length >= 2 && StrUtil.isNotBlank(arg[1])) {
                    res = checkDateString(arg[1]);
                    if (!res.isSuccess())
                        return res;
                    res = parse(arg[1]);
                    if (!res.isSuccess())
                        return res;
                    mapTemp.put("end", arg[1]);
                    mapTemp.put("endDate", res.getObj());
                    mapTemp.put("lastEnd", lastEnd);
                    mapTemp.put("lastEndDate", lastEndDate);
                    lastEnd = arg[1];
                    lastEndDate = (Date)res.getObj();
                } else {
                    mapTemp.put("lastEnd", lastEnd);
                    mapTemp.put("lastEndDate", lastEndDate);
                    lastEnd = "";
                    lastEndDate = null;
                }
            } else if (i != list.size() - 1) {
                return Result.fail("该简历中的"+ key + "时间格式有问题");
            }
            if (StrUtil.isNotBlank(value)) {
                if (i == list.size() - 1)
                    mapTemp.put("isLast", Boolean.valueOf(true));
                res = checkValue(value, mapTemp);
                if (!res.isSuccess())
                    return res;
            } else {
                return Result.fail("该简历中"+ key + "后边没有职务信息");
            }
        }
        return Result.ok();
    }

    private Result checkValue(String value, Map<String, Object> map) {
        boolean isFirst = (map.get("isFirst") == null) ? false : ((Boolean)map.get("isFirst")).booleanValue();
        boolean isLast = (map.get("isLast") == null) ? false : ((Boolean)map.get("isLast")).booleanValue();
        String start = (String)map.get("start");
        String end = (String)map.get("end");
        Date startDate = (Date)map.get("startDate");
        Date endDate = (map.get("endDate") == null) ? null : (Date)map.get("endDate");
        Date lastStartDate = (Date)map.get("lastStartDate");
        Date lastEndDate = (map.get("lastEndDate") == null) ? null : (Date)map.get("lastEndDate");
        if (isLast && endDate == null) {
            endDate = new Date();
            end = DateUtil.format(endDate, "YYYY.MM");
        }
        if (!isLast && (
                startDate == null || endDate == null)) {
            String key = (map.get("key") == null) ? "" : String.valueOf(map.get("key"));
            return new Result(false,"该简历中”"+ key + "  " + value + "“的起始日期或者结束日期为空!");
        }
        Pattern pattern = Pattern.compile("\\d{1}");
        Matcher isNum = null;
        String[] strs = value.split("<br/>");
        Result res = null;
        for (String str : strs) {
            if (StrUtil.isNotBlank(str) && (str.startsWith("(") || str.startsWith("（"))) {
                    String val = str.substring(1);
            if (val.startsWith("期间") || val.startsWith("其间")) {
                if (val.startsWith("期间：") || val.startsWith("期间:") || val.startsWith("其间：") || val.startsWith("其间:")) {
                    val = val.replace("期间：", "").replace("期间:", "").replace("其间：", "").replace("其间:", "");
                    res = checkDateStr(value.replace("<br/>", ""), val, startDate, endDate, lastStartDate, lastEndDate, true, isFirst, isLast);
                    if (!res.isSuccess())
                        return res;
                } else {
                    return new Result(false, "该简历中"+ str + "的期间或者其间后的格式不正确");
                }
            } else if (!StrUtil.isBlank(str)) {
                String tempV = str.substring(0, 1);
                isNum = pattern.matcher(tempV);
                if (isNum.matches()) {
                    if (isFirst) {
                        res = checkDateStr(value.replace("<br/>", ""), val, startDate, endDate, lastStartDate, lastEndDate, true, isFirst, isLast);
                    } else {
                        res = checkDateStr(value.replace("<br/>", ""), val, startDate, endDate, lastStartDate, lastEndDate, false, isFirst, isLast);
                    }
                    if (!res.isSuccess())
                        return res;
                }
            }
        }
    }
    return Result.ok();
}


    private Result checkDateStr(String oriValue, String value, Date start, Date end, Date lastStart, Date lastEnd, boolean isQJ, boolean isFirst, boolean isLast) {
        Pattern pattern = Pattern.compile("\\d{4}\\.\\d{2}--\\d{4}\\.\\d{2}");
        Pattern pattern1 = Pattern.compile("\\d{4}\\.\\d{2}");
        Matcher isNum = null;
        Result res = null;
        if (null == lastEnd && !isFirst)
            return Result.fail("“"+ oriValue + "”上一条简历中的开始年.月为空！");
        Date qjStart = null;
        Date qjEnd = null;
        if (value.length() >= 16) {
            value = value.substring(0, 16);
            isNum = pattern.matcher(value);
            if (isNum.matches()) {
                String[] sts = value.split("--");
                res = checkDateString(sts[0]);
                if (!res.isSuccess())
                    return res;
                qjStart = (Date)res.getObj();
                res = checkDateString(sts[1]);
                if (!res.isSuccess())
                    return res;
                qjEnd = (Date)res.getObj();
                if (isFirst) {
                    if (isQJ) {
                        if (qjStart.getTime() >= start.getTime())
                            return Result.ok();
                        return Result.fail("“"+ value + "”中的起始年月需要在此条简历的开始时间之后");
                    }
                    if (qjStart.getTime() >= lastStart.getTime())
                        return Result.ok();
                    return Result.fail("“"+ value + "”中的起始年月需要在此条简历的开始时间之后");
                }
                if (isQJ) {
                    if (qjStart.getTime() >= start.getTime() && qjEnd.getTime() <= end.getTime() && qjStart.getTime() <= qjEnd.getTime())
                        return Result.ok();
                    return Result.fail("“"+ value + "”中的起始年月需要在此条简历的开始时间之间");
                }
                if (qjStart.getTime() >= lastStart.getTime() && qjEnd.getTime() <= end.getTime() && qjStart.getTime() <= qjEnd.getTime())
                    return Result.ok();
                return Result.fail("“"+  value + "”中的起始年月需要在此条简历的开始时间之间");
            }
            value = value.substring(0, 7);
            isNum = pattern1.matcher(value);
            if (isNum.matches()) {
                res = checkDateString(value);
                if (!res.isSuccess())
                    return res;
                qjStart = (Date)res.getObj();
                if (isLast) {
                    if (qjStart.getTime() >= start.getTime())
                        return Result.ok();
                    return Result.fail("“"+ value + "“中的开始年月需要在这条简历的开始时间之后");
                }
                if (qjStart.getTime() >= start.getTime() && qjStart.getTime() <= end.getTime())
                    return Result.ok();
                return Result.fail("“"+ value + "“中的年月需要在这条简历的开始和这条简历的结束时间之间");
            }
            return Result.fail(oriValue + "中格式不对，应该是“年.月”格式的不是“年.月”的格式");
        }
        return Result.ok();
    }


    private Result parse(String dateStr) {
        if (null != dateStr && StrUtil.isNotBlank(dateStr))
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM");
                return Result.check(sdf.parse(dateStr));
            } catch (Exception e) {
                return Result.fail(dateStr + "不符合年.月格式");
            }
        return Result.ok();
    }
    public Result checkDateString(String str) {
        if (str.indexOf(".") == -1)
            return new Result(false, "+ str + ");
        try {
            SimpleDateFormat sd = new SimpleDateFormat("yyyy.MM");
            sd.setLenient(false);
            Date date = sd.parse(str);
            if (null == date)
                return new Result(false, "+ str + ");
            return new Result(true, date);
        } catch (Exception e) {
            return new Result(false, "+ str + ");
        }
    }

    private List<Map<String, String>> getA1701List(String a1701) {
        if (StrUtil.isBlank(a1701))
            return new ArrayList<>();
        String[] st = a1701.split("\r");
        List<Map<String, String>> listMap = new ArrayList<>();
        String[] sts = null;
        List<Integer> listInt = null;
        for (int j = 0; j < st.length; j++) {
            String str = st[j];
            Map<String, String> map = new HashMap<>();
            String string = "";
            str = str.trim().replaceAll("&nbsp;", " ").replaceAll("    ", " ");
            if (j == st.length - 1 &&
                    sts != null && ResumeEnum.isContain(sts[0])) {
                map.put("key", "");
                map.put("value", str);
                listMap.add(map);
            } else {
                sts = str.split(" ");
                if (sts.length > 0) {
                    map.put("key", sts[0]);
                    if (sts.length > 1) {
                        for (int i = 1; i < sts.length; i++) {
                            String newStr = "";
                            if (StrUtil.isNotBlank(sts[i])) {
                                listInt = new ArrayList<>();
                                listInt = getSpecialSignIndexArray(sts[i], "（", 0, listInt);
                                        listInt = getSpecialSignIndexArray(sts[i], "(", 0, listInt);
                                Collections.sort(listInt);
                                newStr = dealJianliSpecialPartToCheck(sts[i], listInt);
                                string = string + newStr;
                            }
                        }
                        map.put("value", string);
                    } else {
                        map.put("value", string);
                    }
                    listMap.add(map);
                }
            }
        }
        return listMap;
    }

    private String dealJianliSpecialPartToCheck(String str, List<Integer> list) {
        StringBuffer sb = new StringBuffer(str);
        for (int i = list.size() - 1; i >= 0; i--) {
            int in = ((Integer)list.get(i)).intValue();
            sb.insert(in, "<br/>");
            sb.append("  ");
        }
        return sb.toString();
    }


    public static List<Integer> getSpecialSignIndexArray(String str, String sign, int fromIndex, List<Integer> list) {
        int index = str.indexOf(sign, fromIndex);
        if (index != -1) {
            list.add(Integer.valueOf(index));
            return getSpecialSignIndexArray(str, sign, index + 1, list);
        }
        return list;
    }


    private String dealJianliTextPutOffSpecial(String a1701) {
        Pattern pattern = Pattern.compile("\\d{4}\\.\\d{1,2}");
        Matcher isNum = null;
        String[] st = a1701.replaceAll("\\\\n", "\r").replaceAll("\\\\r", "\r").replaceAll("\n", "\r").split("\r");
        List<String> list = new ArrayList<>();
        for (String str : st) {
            if (StrUtil.isNotBlank(str.trim()))
                list.add(str.trim());
        }
        String newStr = "";
        if (list.size() == 1)
            return list.get(0);
        for (int i = 0; i < list.size(); i++) {
            String str = list.get(i);
            if (i != list.size() - 1) {
                if (str.length() >= 7) {
                    isNum = pattern.matcher(str.substring(0, 7) + "");
                    if (isNum.matches()) {
                        if (StrUtil.isBlank(newStr)) {
                            newStr = newStr + str;
                        } else {
                            newStr = newStr + "\r" + str;
                        }
                    } else {
                        newStr = newStr + str;
                    }
                } else {
                    newStr = newStr + str;
                }
            } else if (ResumeEnum.isContain(str)) {
                newStr = newStr + "\r\r" + str;
            } else if (str.length() >= 7) {
                isNum = pattern.matcher(str.substring(0, 7) + "");
                if (isNum.matches()) {
                    newStr = newStr + "\r" + str;
                } else if (ResumeEnum.isContain(str)) {
                    newStr = newStr + "\r\r" + str;
                } else {
                    newStr = newStr + str;
                }
            } else {
                newStr = newStr + str;
            }
        }
        return newStr;
    }

}