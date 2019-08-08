package com.hadoop.bigdata.hadoop.mr.project.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentUtils {
    static Pattern pattern= Pattern.compile("topicId=[0-9]+");
    public static String getPageId(String url){
        String pageId="-";
        if(StringUtils.isBlank(url)){
            return pageId;
        }

        Matcher matcher=pattern.matcher(url);
        if(matcher.find()){
            pageId=matcher.group().split("topicId=")[1];
        }
        return pageId;
    }
}
