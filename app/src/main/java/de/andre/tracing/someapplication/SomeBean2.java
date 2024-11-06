package de.andre.tracing.someapplication;

import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class SomeBean2 {

  public void doSomething() {
    try {
      Thread.sleep(Duration.ofMillis(200));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
