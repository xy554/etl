package com.xy.etl;

import com.xy.etl.cli.support.ConfigFileCliSupport;
import org.springframework.boot.Banner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class EtlApplication {

    public static void main(String[] args) {
        boolean cliMode = ConfigFileCliSupport.isCliMode();
        SpringApplication app = new SpringApplication(EtlApplication.class);
        if (cliMode) {
            app.setWebApplicationType(WebApplicationType.NONE);
            app.setBannerMode(Banner.Mode.OFF);
        }
        try {
            ConfigurableApplicationContext context = app.run(args);
            if (cliMode) {
                System.exit(SpringApplication.exit(context));
            }
        } catch (RuntimeException ex) {
            if (cliMode) {
                System.exit(resolveExitCode(ex));
            }
            throw ex;
        }
    }

    private static int resolveExitCode(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ExitCodeGenerator exitCodeGenerator) {
                return exitCodeGenerator.getExitCode();
            }
            current = current.getCause();
        }
        return 1;
    }
}
