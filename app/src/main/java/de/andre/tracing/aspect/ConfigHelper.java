package de.andre.tracing.aspect;

public class ConfigHelper {

  private final String[] args;

  ConfigHelper(String[] args) {
    this.args = args;
  }

  String value(String name) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + name)) {
        return getArgOrNull(i + 1);
      }
    }

    String vmArg = System.getProperty(name);
    if (vmArg != null) {
      return vmArg;
    }

    return System.getenv(name.toUpperCase().replace('.', '_'));
  }

  String requiredValue(String name) {
    String strValue = value(name);
    if (strValue == null) {
      throw new IllegalArgumentException("Parameter -" + name + " is missing");
    }
    return strValue;
  }

  private String getArgOrNull(int index) {
    if (index >= 0 && index < args.length) {
      return args[index];
    }
    return null;
  }
}
