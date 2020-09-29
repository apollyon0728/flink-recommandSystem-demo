package com.demo.enums;


/**
 * <p>时间格式星期工具类</p>
 *
 * @author sunzhiqiang23
 * @date 2020/1/6 12:22
 */
public enum Week {

    /**
     * 时间格式星期工具类
     */
    MONDAY("星期一", "Monday", "Mon.", 1),  
    TUESDAY("星期二", "Tuesday", "Tues.", 2),  
    WEDNESDAY("星期三", "Wednesday", "Wed.", 3),  
    THURSDAY("星期四", "Thursday", "Thur.", 4),  
    FRIDAY("星期五", "Friday", "Fri.", 5),  
    SATURDAY("星期六", "Saturday", "Sat.", 6),  
    SUNDAY("星期日", "Sunday", "Sun.", 7);  
      
    String nameCn;
    String namen;
    String nameEnShort;
    int number;  
      
    Week(String nameCn, String namen, String nameEnShort, int number) {
        this.nameCn = nameCn;
        this.namen = namen;
        this.nameEnShort = nameEnShort;
        this.number = number;  
    }  
      
    public String getChineseName() {
        return nameCn;
    }  
  
    public String getName() {  
        return namen;
    }  
  
    public String getShortName() {  
        return nameEnShort;
    }  
  
    public int getNumber() {  
        return number;  
    }  
}  
