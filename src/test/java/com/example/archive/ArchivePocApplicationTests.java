package com.example.archive;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class ArchivePocApplicationTests {

	@Test
	void contextLoads() {
	}

}
