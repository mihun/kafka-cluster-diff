package com.kafka.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.stereotype.Service;

@Service
public class StaticContextHolder implements BeanFactoryAware {

    private static BeanFactory context;

    public StaticContextHolder() {
    }

    public static Object getBean(String s) throws BeansException {
        return context.getBean(s);
    }

    public static <T> T getBean(String s, Class<T> tClass) throws BeansException {
        return context.getBean(s, tClass);
    }

    public static <T> T getBean(Class<T> tClass) throws BeansException {
        return context.getBean(tClass);
    }

    public static Object getBean(String s, Object... objects) throws BeansException {
        return context.getBean(s, objects);
    }

    public static boolean containsBean(String s) {
        return context.containsBean(s);
    }


    @Override
    public void setBeanFactory(BeanFactory applicationContext) throws BeansException {
        context = applicationContext;
    }
}