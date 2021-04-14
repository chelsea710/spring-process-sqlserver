package com.zenith.springprocesssqlserver.gbservice;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Date;
import java.util.List;

/**
 * 人员批量修改
 * @date 2019/04/17
 * @author LHR
 */
@Data
@ToString
@NoArgsConstructor
@Accessors(chain = true)
public class BatchUpdateMem {
    /**
     * 层级码
     */
    private String orgCode;
    /**
     * 是否包含下级
     */
    private Boolean isLowelLevel;
    /**
     * 关键字
     */
    private String keyWord;
    /**
     * 管理类别
     */
    private String memType;
    /**
     * 是否全选(1:是全选 0:是不全选)
     */
    private String isSelectAll;
    /**
     * 需要修改的人员
     */
    private List<A0000Dto> mems;
    /**
     * 统计关系所在单位
     */
    private String A0195;
    /**
     * 年度考核
     */
    private Date A1521;
    /**
     * 考核结论
     */
    private String A1517;
    /**
     * 人员类别
     */
    private String A0160;
    /**
     * 管理类别
     */
    private String A0165;
    /**
     * 编制类型
     */
    private String A0121;
    /**
     * 职位类别
     */
    private String A0123;
    /**
     * 是否重新生成年度考核综述
     */
    private String isA15;
    /**
     * 是否重新生成职务综述
     */
    private String isA02;
    /**
     * 是否重新生成学历学位综述
     */
    private String isA08;
    /**
     * 是否重新生成专业技术职务
     */
    private String isA06;
    /**
     * 是否重新生成奖惩综述信息
     */
    private String isA14;

    public Boolean isConditionEmpty(){
        if(StrUtil.isEmpty(A0121) && StrUtil.isEmpty(A0123) && StrUtil.isEmpty(A0195) && ObjectUtil.isNull(A1521) && StrUtil.isEmpty(A1517) && StrUtil.isEmpty(A0160) && StrUtil.isEmpty(A0165) && StrUtil.isEmpty(isA15) && StrUtil.isEmpty(isA02) && StrUtil.isEmpty(isA08) && StrUtil.isEmpty(isA06) && StrUtil.isEmpty(isA14)){
            return true;
        }
        return false;
    }
}
