package com.yling.es.elasticsearch_plugin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.AbstractDoubleSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

/**
 * 基于品种，机构，关注企业，以及发布时间的排序
 * 
 * @author hadoop
 *
 */
public class MyNativeScriptPlugin extends Plugin implements ScriptPlugin {

	private static Logger logger = LogManager.getLogger(MyNativeScriptPlugin.class);

	@Override
	public List<NativeScriptFactory> getNativeScripts() {
		return Collections.singletonList(new MyNativeScriptFactory());
	}

	public static class MyNativeScriptFactory implements NativeScriptFactory {
		@Override
		public ExecutableScript newScript(@Nullable Map<String, Object> params) {
			return new MyNativeScript(params);
		}

		public boolean needsScores() {
			return false;
		}

		public String getName() {
			return "my_script";
		}

	}

	public static class MyNativeScript extends AbstractDoubleSearchScript {

		// 品种分类的权重高的集合
		public List<String> classificationList;

		// 用户的登录的省会的code
		public String userProviCode;

		// 用户登录的城市code
		public String userCityCode;

		// 用户登录的区的code
		public String userCountyCode;

		public String classificationStr;

		public String concernCompanyStr;

		public String[] classifications;

		public String[] concernCompanys;

		/*
		 * 关注企业的集合
		 */
		public List<String> concernCompanyList;

		public MyNativeScript(@Nullable Map<String, Object> params) {
			classificationList = new ArrayList<String>();
			concernCompanyList = new ArrayList<String>();
			classificationStr = (String) params.get("classification");
			userProviCode = (String) params.get("userProviCode");
			userCityCode = (String) params.get("userCityCode");
			userCountyCode = (String) params.get("userCountyCode");
			concernCompanyStr = (String) params.get("concernCompany");
			classifications = classificationStr.split(",");
			for (String classification : classifications) {
				classificationList.add(classification);
			}

			concernCompanys = concernCompanyStr.split(",");
			for (String concernCompany : concernCompanys) {
				concernCompanyList.add(concernCompany);
			}
		}

		@Override
		public double runAsDouble() {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String today = sdf.format(new Date());
			// logger.info("用户传入的参数的字符串是:"+classificationStr);
			try {
				double classificationNumber = 0.0;
				double orgnationNumber = 0.0;
				double concernCompanyNumber;
				double publishDate;
				String doc_classification = (String) source().get("classificationName");

				// 根据用户传入的用户地理位置，省，市，区位置匹配后，获取权重（优先级为第二）
				String doc_provinceCode = (String) source().get("provinceCode");
				String doc_cityCode = (String) source().get("cityCode");
				String doc_countyCode = (String) source().get("countyCode");
				String doc_orginationType = (String) source().get("orginationType");
				String doc_orginationName = (String) source().get("orginationName");		
				// 根据传入的参数分类的有序列表，设置相应的权重分数值（优先级为第一）
				for (int i = 0; i < classificationList.size(); i++) {
					logger.info("文章的类型字符串与用户传入的比较" + classificationList.get(i).equalsIgnoreCase(doc_classification));
					if (classificationList.get(i).equalsIgnoreCase(doc_classification)) {
						classificationNumber = (classificationList.size() - i) * 20;
					}
				}
				// 如果用户传入的参数值为null 将这个值处理一下
				if (null == userCityCode) {
					userCityCode = "";
				}
				if (null == userCountyCode) {
					userCountyCode = "";
				}
				// 如果用户省的位置code值传入的参数也为null将设置为权重为0
				if (null == userProviCode) {
					orgnationNumber = 0.0;
				} else {
					if (doc_orginationType.equalsIgnoreCase("0") && doc_provinceCode.equalsIgnoreCase(userProviCode)) {
						if (doc_cityCode.equalsIgnoreCase("") && doc_countyCode.equalsIgnoreCase("")) {
							orgnationNumber = 15;
						}
						if (doc_cityCode.equalsIgnoreCase(userCityCode) && doc_countyCode.equalsIgnoreCase("")) {
							orgnationNumber = 15;
						} 
						if (doc_cityCode.equalsIgnoreCase(userCityCode) && doc_countyCode.equalsIgnoreCase(userCountyCode)) {
							orgnationNumber = 15;
						}
					}
				}

				// logger.info("文章的 省会code是:"+doc_provinceCode+"这篇文章的机构得分是："+orgnationNumber);

				// 根据用户传过来的关注企业参数计算权重（优先级为第三）
				if (doc_orginationType.equalsIgnoreCase("2") & concernCompanyList.contains(doc_orginationName)) {
					concernCompanyNumber = 6.0;
				} else {
					concernCompanyNumber = 0;
				}

				// 根据文章发布时间与当前时间的比较，来设置合理的权重值（优先级为第四）
				publishDate = source().get("publishDate") == null ? -1
						: (sdf.parse(today).getTime() - sdf.parse(source().get("publishDate").toString()).getTime())
								/ (24 * 60 * 60 * 1000);
				if (publishDate >= 620) {
					publishDate = 0.5;
				} else if (publishDate >= 360) {
					publishDate = 1;
				} else if (publishDate >= 180) {
					publishDate = 1.5;
				} else if (publishDate >= 120) {
					publishDate = 2;
				} else if (publishDate >= 90) {
					publishDate = 2.5;
				} else if (publishDate >= 60) {
					publishDate = 3;
				} else if (publishDate >= 30) {
					publishDate = 3.5;
				} else if (publishDate >= 14) {
					publishDate = 4;
				} else if (publishDate >= 7) {
					publishDate = 4.5;
				} else if (publishDate >= 0) {
					publishDate = 5;
				} else {
					publishDate = 0;
				}

				logger.info("品种的种类得分：" + classificationNumber);
				logger.info("机构的得分：" + orgnationNumber);
				logger.info("关注企业的得分" + concernCompanyNumber);
				logger.info("时间顺序的得分" + publishDate);

				// 设置各种排序字段的权重因子
//				double iw_classificationNumber = 0.9;
//				double iw_orgnationNumber = 0.7;
//				double iw_concernCompanyNumber = 0.4;
//				double iw2_publishdate = 0.2;

				// 计算各个权重字段的总和，算出一个平均值作为这篇文档的score
//				double sumW = iw_classificationNumber + iw_orgnationNumber + iw_concernCompanyNumber + iw2_publishdate;
//				double sumScore = (classificationNumber * iw_classificationNumber)
//						+ (orgnationNumber * iw_orgnationNumber) + (concernCompanyNumber * iw_concernCompanyNumber)
//						+ (publishDate * iw2_publishdate);
				double sumScore = classificationNumber + orgnationNumber + concernCompanyNumber + publishDate;
//				return (sumScore / sumW);
				return sumScore;
			} catch (Exception e) {
				logger.error("解析时间有错误");
				return -1;
			}
		}
	}
}
