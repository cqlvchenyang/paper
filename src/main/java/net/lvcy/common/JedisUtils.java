package net.lvcy.common;

import redis.clients.jedis.Jedis;

public class JedisUtils {

	private static Jedis jedis;
	private JedisUtils(){};
	public static Jedis getJedis(){
		if(jedis==null){
			jedis=new Jedis("10.0.0.21", 6379);
		}
		return jedis;
	}
}
