package de.andre.tracing.someapplication;

import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class SomeBean1 {

  public void doSomething() {
    try {
      Thread.sleep(Duration.ofMillis(500));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
