package com.example.archive;

import org.springframework.boot.SpringApplication;

public class TestArchivePocApplication {

	public static void main(String[] args) {
		SpringApplication.from(ArchivePocApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
