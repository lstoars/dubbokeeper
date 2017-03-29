package com.dubboclub.dk.web.controller;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.dubboclub.dk.admin.model.Consumer;
import com.dubboclub.dk.admin.model.Provider;
import com.dubboclub.dk.admin.service.ConsumerService;
import com.dubboclub.dk.admin.service.ProviderService;
import com.dubboclub.dk.admin.sync.util.SyncUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by fei on 2017/3/27.
 */
@Controller
@RequestMapping("/warns")
public class WarnsController {

	@Autowired
	private ProviderService providerService;
	@Autowired
	private ConsumerService consumerService;

	/**
	 * 没有提供方的服务
	 *
	 * @throws IOException
	 */
	@RequestMapping("noprovider")
	public void noprovider(HttpServletResponse response) throws IOException {
		List<Provider> providerServices = providerService.listAllProvider();
		List<Consumer> consumerServices = consumerService.listAllConsumer();
		Set<String> services = new TreeSet<String>();

		List<String> providers = new ArrayList<String>();
		if (providerServices != null) {
			for (Provider provider : providerServices) {
				services.add(provider.getServiceKey());
				providers.add(provider.getServiceKey());
			}
		}
		if (consumerServices != null) {
			for (Consumer provider : consumerServices) {
				services.add(provider.getServiceKey());
			}
		}
		StringBuilder noProviderService = new StringBuilder();
		for (String service : services) {
			if (!providers.contains(service)) {
				noProviderService.append(service).append(",");
			}
		}
		if (StringUtils.isNotEmpty(noProviderService.toString())) {
			response.getWriter()
					.println(noProviderService.deleteCharAt(noProviderService.length() - 1).toString());
		} else {
			response.getWriter().println("1");
		}
	}

	/**
	 * @param ip
	 * @throws IOException
	 * @功能描述: 根据IP查询服务器的提供接口以及消费接口
	 * @创建作者: 欧阳文斌
	 * @创建日期: 2016年12月5日 上午10:28:42
	 */
	@RequestMapping("queryByIp")
	@ResponseBody
	public HashMap<String, Object> queryByIp(String ip, String port) throws IOException {
		HashMap<String, Object> result = new HashMap<String, Object>();
		if (StringUtils.isEmpty(ip)) {
			result.put("status", -1);
			result.put("msg", "参数错误");
		}
		if (StringUtils.isEmpty(port)) port = "20880";
		String providerIp = ip + ":" + port;
		//默认端口  如果一台服务器多个提供者，需要拓展这里的方法
		List<Provider> providers = providerService.listProviderByConditions(SyncUtils.ADDRESS_FILTER_KEY, providerIp);
		List<Consumer> consumers = consumerService.listConsumerByConditions(SyncUtils.ADDRESS_FILTER_KEY, ip);

		List<CheckServiceModel> providerResult = new ArrayList<WarnsController.CheckServiceModel>();
		if (providers != null) {
			for (Provider provider : providers) {
				CheckServiceModel model = new CheckServiceModel();
				model.setUrl(provider.getUrl());
				providerResult.add(model);
			}
		}
		result.put("providerDatas", providerResult);
		List<CheckServiceModel> consumerResult = new ArrayList<WarnsController.CheckServiceModel>();
		if (consumers != null) {
			for (Consumer consumer : consumers) {
				CheckServiceModel model = new CheckServiceModel();
				model.setService(consumer.getServiceKey());
				consumerResult.add(model);
			}
		}
		result.put("consumerDatas", consumerResult);
		result.put("status", 1);
		result.put("msg", "调用成功");
		return result;
	}


	/**
	 * @param ip
	 * @throws IOException
	 * @功能描述: 根据接口名查询提供者的URL
	 * @创建作者: 欧阳文斌
	 * @创建日期: 2016年12月5日 上午10:28:42
	 */
	@RequestMapping("queryByService")
	@ResponseBody
	public HashMap<String, Object> queryByService(Map<String, Object> context) throws IOException {
		HashMap<String, Object> result = new HashMap<String, Object>();
		if (context == null) {
			result.put("status", -1);
			result.put("msg", "参数错误");
		}
		String interFace = (String) context.get("interface");
		if (StringUtils.isEmpty(interFace)) {
			result.put("status", -1);
			result.put("msg", "参数错误");
		}
		List<Provider> providers = providerService.listProviderByService(interFace);

		List<CheckServiceModel> providerResult = new ArrayList<WarnsController.CheckServiceModel>();
		if (providers != null) {
			for (Provider provider : providers) {
				CheckServiceModel model = new CheckServiceModel();
				model.setUrl(provider.getUrl());
				providerResult.add(model);
			}
		}
		result.put("providerDatas", providerResult);
		result.put("status", 1);
		result.put("msg", "调用成功");
		return result;
	}

	class CheckServiceModel implements Serializable {
		private static final long serialVersionUID = 1L;
		/**
		 * dubbo url
		 */
		private String url;
		/**
		 * 接口
		 */
		private String service;

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getService() {
			return service;
		}

		public void setService(String service) {
			this.service = service;
		}
	}
}
