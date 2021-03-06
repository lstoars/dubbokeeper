package com.dubboclub.dk.admin.service;

import com.dubboclub.dk.admin.model.Application;
import com.dubboclub.dk.admin.model.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bieber on 2015/6/3.
 */
public interface ApplicationService {

    //获取当前注册中心所有应用列表
    public List<Application> getApplications();
    //获取某个应用部署节点信息
    public List<Node> getNodesByApplicationName(String appName);
    //获取所有节点对应的IP
    public Map<String,Set<String>> listAllApplicationIps();
}
