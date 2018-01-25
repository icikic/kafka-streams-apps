package icikic.kstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class KstreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(KstreamsApplication.class, args);
	}



}
