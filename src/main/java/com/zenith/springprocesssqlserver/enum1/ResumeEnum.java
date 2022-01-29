package com.zenith.springprocesssqlserver.enum1;

import java.util.regex.Pattern;

public enum ResumeEnum {
    VALUE1("全国人大常委","START_WITH",""),
    VALUE2("全国政协委员","START_WITH",""),
    VALUE3("全国政协常委","START_WITH",""),
    VALUE4("中央委员","LIKE",""),
    VALUE5("候补委员","LIKE",""),
    VALUE6("中央纪委委员","LIKE",""),
    VALUE7("全国人大常委会委员","LIKE",""),
    VALUE8("全国政协常务委员","LIKE",""),
    VALUE9("人大代表","LIKE","");

    public String value;

    public String type;

    public String sysunit;

    ResumeEnum(String value, String type, String sysunit) {
        this.value = value;
        this.type = type;
        this.sysunit = sysunit;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getSysunit() {
        return this.sysunit;
    }

    public void setSysunit(String sysunit) {
        this.sysunit = sysunit;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public static boolean isContain(String str) {
        if (str.startsWith("(") || str.startsWith("）"))
        return false;
        Pattern pattern = Pattern.compile("\\d{4}\\.\\d{1,2}");
        if (str.length() >= 7 && pattern.matcher(str.substring(0, 7) + "").matches())
            return false;
        Pattern p = Pattern.compile("\\(\\d{4}\\.\\d{1,2}-{2}\\d{4}\\.\\d{1,2}");
        if (str.length() >= 17 && p.matcher(str.substring(0, 17) + "").matches())
            return false;
        return true;
    }
}
