package com.ipjmc.mapr.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * 
 * @author wylazy
 *
 */
public class WylazyConfigured extends Configured {

	static {
		Configuration.addDefaultResource("hadoop-localhost.xml");
	}
}
