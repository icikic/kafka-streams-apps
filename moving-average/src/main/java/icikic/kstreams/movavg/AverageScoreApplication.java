package icikic.kstreams.movavg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class AverageScoreApplication {

	public static void main(String[] args) {
		SpringApplication.run(AverageScoreApplication.class, args);
	}

}
