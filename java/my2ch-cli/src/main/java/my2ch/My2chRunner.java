package my2ch;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class My2chRunner implements CommandLineRunner, ExitCodeGenerator
{
    private int exitCode;

    public static void main(String[] args)
    {
        // let Spring instantiate and inject dependencies
        System.exit(SpringApplication.exit(SpringApplication.run(My2chRunner.class, args)));
    }

    @Override
    public void run(String... args)
    {
        // let picocli parse command line args and run the business logic
        //exitCode = new CommandLine(mailCommand, factory).execute(args);
    }

    @Override
    public int getExitCode()
    {
        return exitCode;
    }
}