package com.zenith.springprocesssqlserver.pojo;

import com.alibaba.fastjson.JSON;

public class Result {

    private boolean success;

    private String msg;

    private Object obj;

    public Result() {}

    public Result(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }

    public Result(boolean success) {
        this.success = success;
    }

    public Result(boolean success, Object obj) {
        this.success = success;
        this.obj = obj;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsg() {
        return this.msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getObj() {
        return this.obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

    public static Result ok() {
        return new Result(true);
    }

    public static Result fail(String msg) {
        return new Result(false, msg);
    }

    public static Result check(Object obj) {
        return new Result(true, obj);
    }



    public static String okJson() {
        return JSON.toJSONString(new Result(true));
    }


    public static String failJson() {
        return JSON.toJSONString(new Result(false));
    }


    public String toString() {
        return "Result [success=" + this.success + ", msg=" + this.msg + "]";
    }

    public static String toJsonBySuccess(boolean success) {
        if (success)
            return okJson();
        return failJson();
    }

}
