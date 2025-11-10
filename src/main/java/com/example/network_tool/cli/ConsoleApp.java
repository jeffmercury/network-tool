package com.example.network_tool.cli;

import com.example.network_tool.service.ProfileService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

@Component
public class ConsoleApp implements CommandLineRunner {
    private final ProfileService service;

    public ConsoleApp(ProfileService service) { this.service = service; }

    @Override
    public void run(String... args) {
        System.out.println("== POI Network Tool ==");
        System.out.println("Enter POI SSN (or 'exit'):");
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> SSN: ");
                String ssn = sc.nextLine().trim();
                if (ssn.equalsIgnoreCase("exit")) break;
                if (ssn.isEmpty()) continue;

                try {
                    String json = service.profileJson(ssn);
                    System.out.println(json);
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
        }
    }
}
