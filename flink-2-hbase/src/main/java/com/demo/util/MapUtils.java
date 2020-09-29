package com.demo.util;

import com.demo.domain.User;
import org.apache.commons.collections.map.LinkedMap;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Map;

/**
 * @author iceter
 */
public class MapUtils {

    public static Object transMap(Class type, Map map) throws Exception {


        Object obj = type.newInstance();//实例化类
        BeanInfo info = Introspector.getBeanInfo(type);//获取类中属性

        PropertyDescriptor[] propertyPermissions = info.getPropertyDescriptors();

        for (PropertyDescriptor pro : propertyPermissions) {

            String proName = pro.getName();
            if(map.containsKey(proName)){
                Object methodName = map.get(proName);
                Object[] args = {methodName};
                try {
                    pro.getWriteMethod().invoke(obj,args);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

        }

        return obj;
    }

    public static void main(String[] args) throws Exception {
        Map map = new LinkedMap();
        map.put("age",11);
        map.put("id","33");
        map.put("sex","man");
        map.put("name","fei");
        map.put("addr","neyok");

        User user =  (User) MapUtils.transMap(User.class,map);
        System.out.println(user);
    }

}
