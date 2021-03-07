package com.ethlo.my2ch;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import picocli.CommandLine;

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
        exitCode = new CommandLine(new TransferCommand()).execute(args);
    }

    @Override
    public int getExitCode()
    {
        return exitCode;
    }
}