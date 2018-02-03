package icikic.kstreams.pageview;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class TopNPagesPerCountryApplication {

	public static void main(String[] args) {
		SpringApplication.run(TopNPagesPerCountryApplication.class, args);
	}

}
