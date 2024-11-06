package de.andre.tracing.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LoggingAspect {

  @Pointcut("""
         execution(* de.andre.tracing.someapplication..*(..))
      || execution(* de.andre.tracing.application..*(..))
      """)
  public void allServiceMethods() {
  }

  @Around("allServiceMethods()")
  public Object logAround(ProceedingJoinPoint joinPoint) throws Throwable {
    long startNs = System.nanoTime();

    try {
      return joinPoint.proceed();
    } finally {
      long durationNs = System.nanoTime() - startNs;
      String signature = joinPoint.getSignature().toString();
      Thread currentThread = Thread.currentThread();
      int threadHashCode = currentThread.hashCode();
      String threadName = currentThread.getName();
      TracingService.trace(startNs, durationNs, signature, threadHashCode, threadName);
    }
  }
}