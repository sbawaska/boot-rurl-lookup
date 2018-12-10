package io.projectriff.demo.rurllookup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.function.Function;

@SpringBootApplication
public class RurlLookupApplication {

    private static final String DOMAIN_NAME = "http://r.url/";

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Bean
    public StringRedisTemplate stringRedisTemplate() {
        return new StringRedisTemplate(redisConnectionFactory());
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        // TODO parameterize this
        return new LettuceConnectionFactory(new RedisStandaloneConfiguration(
                "my-redis-master.default.svc.cluster.local"));
    }

    @Bean
    public Function<String, String> lookupUrl() {
        return s -> {
            System.out.println("received request:"+s);
            String hash = getHashFromUrl(s);
            return lookupFromRedis(hash);
        };
    }

    private String lookupFromRedis(String hash) {
        return redisTemplate.opsForValue().get(hash);
    }

    private String getHashFromUrl(String shortUrl) {
        if (shortUrl == null || shortUrl.length() < DOMAIN_NAME.length()) {
            throw new IllegalArgumentException("Not a valid shortened url: "+shortUrl);
        }
        return shortUrl.substring(DOMAIN_NAME.length());
    }

    public static void main(String[] args) {
		SpringApplication.run(RurlLookupApplication.class, args);
	}
}
