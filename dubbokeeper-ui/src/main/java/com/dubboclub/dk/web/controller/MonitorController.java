package com.dubboclub.dk.web.controller;

import java.util.*;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.dubboclub.dk.admin.model.Provider;
import com.dubboclub.dk.admin.service.ApplicationService;
import com.dubboclub.dk.admin.service.ProviderService;
import com.dubboclub.dk.storage.StatisticsStorage;
import com.dubboclub.dk.storage.model.*;
import com.dubboclub.dk.web.model.MethodStatistics;
import com.dubboclub.dk.web.utils.ConfigUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/monitor")
public class MonitorController {

    @Autowired
    @Qualifier("statisticsStorage")
    private StatisticsStorage statisticsStorage ;

    @Autowired
    private ProviderService providerService;

    @Autowired
    private ApplicationService applicationService;

    private static final long ONE_DAY=24*60*60*1000;

    private static final long ONE_HOUR=60*60*1000;

    @RequestMapping("/load-interval.htm")
    public @ResponseBody Integer queryInterval(){
        return Integer.parseInt(ConfigUtils.getProperty("monitor.collect.interval"));
    }

    @RequestMapping("/{application}/{service}/{method}/now.htm")
    public @ResponseBody
    MethodStatistics queryCurrentMethodStatistics(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("method")String method){
        MethodStatistics methodStatistics = new MethodStatistics();
        long currentTime = System.currentTimeMillis();
        methodStatistics.setStatisticsCollection(statisticsStorage.queryStatisticsForMethod(application,service,method,currentTime-ONE_HOUR,currentTime));
        return methodStatistics;
    }

    @RequestMapping("/{application}/{service}/{method}/{startTime}-{endTime}/monitors.htm")
    public @ResponseBody
    MethodStatistics queryMethodStatistics(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("method")String method,@PathVariable("startTime")long startTime,@PathVariable("endTime") long endTime){
        MethodStatistics methodStatistics = new MethodStatistics();
        methodStatistics.setStatisticsCollection(statisticsStorage.queryStatisticsForMethod(application,service,method,startTime,endTime));
        return methodStatistics;
    }

	@RequestMapping("/{application}/{service}/{method}/{startTime}-{endTime}/invoker.htm")
	public @ResponseBody
	List<AddressCount> queryMethodInvokerCount(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("method")String method,@PathVariable("startTime")long startTime,@PathVariable("endTime") long endTime) {
		return convertAddressCount(statisticsStorage.queryMethodInvokerCount(application,service,method,startTime,endTime));
	}

    @RequestMapping("/{application}/{service}/now.htm")
    public @ResponseBody
    Collection<MethodMonitorOverview> overviewServiceRealTime(@PathVariable("application")String application,@PathVariable("service")String service){
        List<Provider> providers = providerService.listProviderByServiceKey(service);
        List<String> methods = new ArrayList<String>();
        if(providers.size()>0){
            Provider provider = providers.get(0);
            Map<String,String> params = StringUtils.parseQueryString(provider.getParameters());
            String methodStr = params.get(Constants.METHODS_KEY);
            if(!StringUtils.isEmpty(methodStr)){
                String[] methodArray = Constants.COMMA_SPLIT_PATTERN.split(methodStr);
                for(String method:methodArray){
                    methods.add(method);
                }
            }
        }
        long currentTime = System.currentTimeMillis();
        return statisticsStorage.queryMethodMonitorOverview(application,service,methods.size(),currentTime-ONE_HOUR,currentTime);
    }
    @RequestMapping("/{application}/{service}/{startTime}-{endTime}/monitors.htm")
    public @ResponseBody
    Collection<MethodMonitorOverview> overviewService(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("startTime")long startTime,@PathVariable("endTime") long endTime){
        List<Provider> providers = providerService.listProviderByServiceKey(service);
        List<String> methods = new ArrayList<String>();
        if(providers.size()>0){
            Provider provider = providers.get(0);
            Map<String,String> params = StringUtils.parseQueryString(provider.getParameters());
            String methodStr = params.get(Constants.METHODS_KEY);
            if(!StringUtils.isEmpty(methodStr)){
                String[] methodArray = Constants.COMMA_SPLIT_PATTERN.split(methodStr);
                for(String method:methodArray){
                    methods.add(method);
                }
            }
        }
        return statisticsStorage.queryMethodMonitorOverview(application,service,methods.size(),startTime,endTime);
    }

    @RequestMapping("/index.htm")
    public @ResponseBody Collection<ApplicationInfo> monitorIndex(){
        return statisticsStorage.queryApplications();
    }

    @RequestMapping("/{application}/{dayRange}/info.htm")
    public @ResponseBody
    ApplicationInfo queryApplicationInfo(@PathVariable("application")String application,@PathVariable("dayRange")int dayRange){
        long currentTime = System.currentTimeMillis();
        return statisticsStorage.queryApplicationInfo(application,currentTime-ONE_DAY*dayRange,currentTime);
    }

    @RequestMapping("/{application}/{dayRange}/overview.htm")
    public @ResponseBody
    StatisticsOverview queryApplicationOverview(@PathVariable("application")String application,@PathVariable("dayRange")int dayRange){
        long currentTime = System.currentTimeMillis();
        return setApplicationName(statisticsStorage.queryApplicationOverview(application,currentTime-(dayRange*ONE_DAY),currentTime));
    }

	@RequestMapping("/{application}/{dayRange}/invoker.htm")
	public @ResponseBody
	List<AddressCount> queryApplicationInvokerCount(@PathVariable("application")String application,@PathVariable("dayRange")int dayRange) {
		long currentTime = System.currentTimeMillis();
		return convertAddressCount(statisticsStorage.queryApplicationInvokerCount(application,currentTime-(dayRange*ONE_DAY),currentTime));
	}

    @RequestMapping("/{application}/{service}/{dayRange}/overview.htm")
    public @ResponseBody
    StatisticsOverview queryServiceOverview(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("dayRange")int dayRange){
        long currentTime = System.currentTimeMillis();
        return setApplicationName(statisticsStorage.queryServiceOverview(application, service, currentTime - (dayRange * ONE_DAY), currentTime));
    }

	@RequestMapping("/{application}/{service}/{dayRange}/invoker.htm")
	public @ResponseBody
	List<AddressCount> queryServiceInvokerCount(@PathVariable("application")String application,@PathVariable("service")String service,@PathVariable("dayRange")int dayRange) {
		long currentTime = System.currentTimeMillis();
		return convertAddressCount(statisticsStorage.queryServiceInvokerCount(application, service, currentTime - (dayRange * ONE_DAY), currentTime));
	}


    @RequestMapping("/{application}/{dayRange}/services.htm")
    public @ResponseBody
    Collection<ServiceInfo> queryServiceByApp(@PathVariable("application")String application,@PathVariable("dayRange")int dayRange){
        long end = System.currentTimeMillis();
        long start = end-ONE_DAY*dayRange;
        return statisticsStorage.queryServiceByApp(application,start,end);
    }

    private List<AddressCount> convertAddressCount(List<AddressCount> addressCounts) {
		Map<String, Set<String>> applicationIps = applicationService.listAllApplicationIps();
		Map<String,Integer> appCounts = new HashMap<String, Integer>();
		for (AddressCount addressCount : addressCounts) {
			String application = getApplicationName(addressCount.getRemoteAddress(), applicationIps);
			Integer count = appCounts.get(application);
			if (count == null) {
				count = addressCount.getCount();
			} else {
				count += addressCount.getCount();
			}
			appCounts.put(application, count);
		}
		List<AddressCount> result = new ArrayList<AddressCount>();
		for (Map.Entry<String,Integer> entry : appCounts.entrySet()) {
			result.add(new AddressCount(entry.getKey(),entry.getValue()));
		}
		return result;
	}

    private StatisticsOverview setApplicationName(StatisticsOverview overview) {
		Map<String, Set<String>> applicationIps = applicationService.listAllApplicationIps();
		List<ConcurrentItem> concurrentItems = overview.getConcurrentItems();
		if (!CollectionUtils.isEmpty(concurrentItems)) {
			for (ConcurrentItem item : concurrentItems) {
				item.setApplication(getApplicationName(item.getRemoteAddr(), applicationIps));
			}
		}
		List<SuccessItem> successItems = overview.getSuccessItems();
		if (!CollectionUtils.isEmpty(successItems)) {
			for (SuccessItem item : successItems) {
				item.setApplication(getApplicationName(item.getRemoteAddr(), applicationIps));
			}
		}
		List<ElapsedItem> elapsedItems = overview.getElapsedItems();
		if (!CollectionUtils.isEmpty(elapsedItems)) {
			for (ElapsedItem item : elapsedItems) {
				item.setApplication(getApplicationName(item.getRemoteAddr(), applicationIps));
			}
		}
		List<FaultItem> faultItems = overview.getFaultItems();
		if (!CollectionUtils.isEmpty(faultItems)) {
			for (FaultItem item : faultItems) {
				item.setApplication(getApplicationName(item.getRemoteAddr(), applicationIps));
			}
		}
		return overview;
	}

	private String getApplicationName(String address,Map<String, Set<String>> applicationIps) {
    	StringBuilder application = new StringBuilder();
		for (Map.Entry<String, Set<String>> entry : applicationIps.entrySet()) {
			if(entry.getValue().contains(address)) {
				if(StringUtils.isEmpty(entry.getKey())) {
					application.append(address).append(",");
				} else {
					application.append(entry.getKey()).append(",");
				}
			}
		}
		if(StringUtils.isEmpty(application.toString())) {
			return address;
		} else {
			return application.deleteCharAt(application.length()-1).toString();
		}

	}

}
