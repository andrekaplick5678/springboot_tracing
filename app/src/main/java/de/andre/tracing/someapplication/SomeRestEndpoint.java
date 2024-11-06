package de.andre.tracing.someapplication;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SomeRestEndpoint {

  private final SomeBean1 someBean1;
  private final SomeBean2 someBean2;

  public SomeRestEndpoint(SomeBean1 someBean1, SomeBean2 someBean2) {
    this.someBean1 = someBean1;
    this.someBean2 = someBean2;
  }

  @GetMapping(path = "/doit")
  public String getSomething() {
    someBean1.doSomething();
    someBean2.doSomething();
    return "done";
  }
}
